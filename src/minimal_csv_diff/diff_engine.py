import polars as pl
import os
from typing import List, Any
from .csv_processor import normalize_string

def process_unique_row_polars(df_unique: pl.DataFrame, source_file_name: str, key_columns: List[str], final_columns: List[str]) -> pl.DataFrame:
    """
    Processes unique rows identified during CSV comparison, ensuring consistent output columns.

    Args:
        df_unique (pl.DataFrame): DataFrame containing rows unique to one file.
        source_file_name (str): Name of the source file (file1 or file2).
        key_columns (List[str]): List of column names used as composite key.
        final_columns (List[str]): The exact list of columns expected in the final output DataFrame.

    Returns:
        pl.DataFrame: DataFrame with unique rows formatted for the diff output, with consistent columns.
    """
    if df_unique.is_empty():
        return pl.DataFrame()

    surrogate_key_expr = pl.concat_str([
        pl.col(col).map_elements(normalize_string, return_dtype=pl.Utf8)
        for col in key_columns if col in df_unique.columns
    ], separator='|')

    # Prepare the DataFrame with fixed columns and then select/fill based on final_columns
    processed_df = df_unique.with_columns([
        surrogate_key_expr.alias('surrogate_key'),
        pl.lit(source_file_name).alias('source'),
        pl.lit(['UNIQUE ROW']).alias('failed_columns'),
    ])

    # Ensure all final_columns are present, filling with nulls if not
    # and select them in the correct order
    select_expressions = []
    for col in final_columns:
        if col in processed_df.columns:
            select_expressions.append(pl.col(col))
        else:
            select_expressions.append(pl.lit(None).alias(col))
            
    return processed_df.select(select_expressions)


def diff_csv_core(df1: pl.DataFrame, df2: pl.DataFrame, file1_name: str, file2_name: str, delimiter: str, key_columns: List[str], output_file: str):
    """
    Core logic for comparing two Polars DataFrames and generating a diff report.
    This function expects already loaded and partially normalized DataFrames.
    """
    # Build the column pool
    column_pool = list(set(df1.columns).union(df2.columns))

    # Build the common pool (columns present in both)
    common_pool = [col for col in column_pool if col in df1.columns and col in df2.columns]
    
    non_key_cols = [col for col in common_pool if col not in key_columns]
    non_key_cols.sort()
    print(f"DEBUG: key_columns: {key_columns}")
    print(f"DEBUG: common_pool: {common_pool}")
    print(f"DEBUG: non_key_cols: {non_key_cols}")

    # Define the final column order for consistency across all output DataFrames
    final_columns = ['surrogate_key', 'source', 'failed_columns'] + key_columns + [col for col in common_pool if col not in key_columns]

    # Step 1: Isolate unique rows using anti-joins
    # Rows unique to file1
    df1_only = df1.join(df2, on=key_columns, how='anti')
    output_df_left_only = process_unique_row_polars(df1_only, file1_name, key_columns, final_columns)

    # Rows unique to file2
    df2_only = df2.join(df1, on=key_columns, how='anti')
    output_df_right_only = process_unique_row_polars(df2_only, file2_name, key_columns, final_columns)

    # Step 2: Isolate and compare modified rows using an inner join
    both_df = df1.join(df2, on=key_columns, how='inner', suffix='_file2')

    # Compare non-key columns to identify modifications
    if not both_df.is_empty():
        diff_checks = []
        for col in non_key_cols:
            val1_expr = pl.col(col)
            val2_expr = pl.col(f'{col}_file2')
            
            # Check for differences, considering nulls
            diff_expr = (
                (val1_expr.is_null() & val2_expr.is_not_null()) |
                (val1_expr.is_not_null() & val2_expr.is_null()) |
                (val1_expr.map_elements(normalize_string, return_dtype=pl.Utf8) != val2_expr.map_elements(normalize_string, return_dtype=pl.Utf8))
            )
            diff_checks.append(
                pl.when(diff_expr)
                .then(pl.lit(col))
                .otherwise(pl.lit(None))
            )

        both_df = both_df.with_columns(
            pl.concat_list(diff_checks).list.drop_nulls().alias('failed_columns')
        )

        both_df_diff = both_df.filter(pl.col('failed_columns').list.len() > 0)

        if not both_df_diff.is_empty():
            surrogate_key_expr = pl.concat_str([pl.col(col).map_elements(normalize_string, return_dtype=pl.Utf8) for col in key_columns], separator='|')

            # Construct file1_diff_rows ensuring correct columns and order
            file1_diff_rows = both_df_diff.with_columns([
                surrogate_key_expr.alias('surrogate_key'),
                pl.lit(file1_name).alias('source'),
            ]).select(
                ['surrogate_key', 'source', 'failed_columns'] +
                [pl.col(col) for col in key_columns] +
                [pl.col(col) for col in non_key_cols]
            ).select(final_columns) # Final selection to enforce order and fill missing


            # Construct file2_diff_rows ensuring correct columns and order
            file2_diff_rows = both_df_diff.with_columns([
                surrogate_key_expr.alias('surrogate_key'),
                pl.lit(file2_name).alias('source'),
            ]).select(
                ['surrogate_key', 'source', 'failed_columns'] +
                [pl.col(col) for col in key_columns] +
                [pl.col(f'{col}_file2').alias(col) for col in non_key_cols]
            ).select(final_columns) # Final selection to enforce order and fill missing

            # Step 3: Combine and finalize the results
            output_df = pl.concat([output_df_left_only, output_df_right_only, file1_diff_rows, file2_diff_rows])
        else:
            output_df = pl.concat([output_df_left_only, output_df_right_only])
    else:
        output_df = pl.concat([output_df_left_only, output_df_right_only])
    if output_df.is_empty():
        return False, None, {
            'total_differences': 0,
            'unique_rows': 0,
            'modified_rows': 0,
            'files_compared': [file1_name, file2_name],
            'common_columns': len(common_pool),
            'key_columns_used': key_columns
        }

    # Convert 'failed_columns' list to string just before writing to CSV
    output_df = output_df.with_columns(
        pl.col('failed_columns').list.join('| - |').alias('failed_columns')
    )
    
    output_df = output_df.sort(by='surrogate_key')

    output_df.write_csv(output_file, separator=',', quote_style="always")
    
    summary = {
        'total_differences': output_df.height,
        'unique_rows': output_df.filter(pl.col('failed_columns') == 'UNIQUE ROW').height,
        'modified_rows': output_df.filter(pl.col('failed_columns') != 'UNIQUE ROW').height,
        'files_compared': [file1_name, file2_name],
        'common_columns': len(common_pool),
        'key_columns_used': key_columns
    }
    
    return True, output_file, summary