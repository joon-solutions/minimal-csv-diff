import polars as pl
import re
from typing import Any, List

def normalize_string(s: Any) -> str:
    """
    Normalizes a string by stripping whitespace, standardizing internal spaces,
    and handling None values.
    
    NOTE: This Python function is kept for backwards compatibility and edge cases.
    For performance-critical paths, use normalize_column_expr() instead.
    """
    if s is None:
        return ""
    s = str(s)
    # Split by lines, strip each line, then join back with original newlines
    lines = s.splitlines()
    normalized_lines = []
    for line in lines:
        # Replace multiple spaces/tabs with a single space within each line
        normalized_line = re.sub(r'[ \t]+', ' ', line.strip())
        normalized_lines.append(normalized_line)
    return '\n'.join(normalized_lines)


def normalize_column_expr(col_name: str) -> pl.Expr:
    """
    Returns a Polars expression that normalizes a string column using native Rust operations.
    
    This is 10-100x faster than map_elements(normalize_string) for large DataFrames.
    
    Normalization:
    - Replaces NULL with empty string
    - Strips leading/trailing whitespace
    - Replaces multiple spaces/tabs with single space
    
    Note: Does not handle per-line stripping for multiline strings (minor difference
    from normalize_string). For CSV diff purposes, this is acceptable since we're
    comparing normalized values from both files identically.
    """
    return (
        pl.col(col_name)
        .fill_null("")
        .str.strip_chars()  # Strip leading/trailing whitespace
        .str.replace_all(r"[ \t]+", " ")  # Replace multiple spaces/tabs with single space
    )

def load_and_normalize_dfs(file1: str, file2: str, delimiter: str, key_columns: List[str]):
    """
    Loads two CSV files into Polars DataFrames, forces Utf8 schema,
    replaces empty strings with nulls, filters all-null rows,
    and normalizes key columns.
    """
    df1 = pl.read_csv(file1, separator=delimiter, infer_schema=False)
    df2 = pl.read_csv(file2, separator=delimiter, infer_schema=False)

    # Replace empty strings with None (null) after ensuring all are Utf8
    df1 = df1.with_columns([
        pl.when(pl.col(c) == "").then(pl.lit(None)).otherwise(pl.col(c)).alias(c)
        for c in df1.columns
    ])
    df2 = df2.with_columns([
        pl.when(pl.col(c) == "").then(pl.lit(None)).otherwise(pl.col(c)).alias(c)
        for c in df2.columns
    ])

    # Filter out rows where all common columns are null
    # This handles cases like empty lines in CSVs that become all nulls
    df1 = df1.filter(~pl.all_horizontal([pl.col(c).is_null() for c in df1.columns]))
    df2 = df2.filter(~pl.all_horizontal([pl.col(c).is_null() for c in df2.columns]))

    # Normalize key columns before merging to ensure proper matching
    # Use native Polars expressions for 10-100x speedup
    df1_normalized_keys = df1.with_columns([
        normalize_column_expr(col).alias(col)
        for col in key_columns if col in df1.columns
    ])
    df2_normalized_keys = df2.with_columns([
        normalize_column_expr(col).alias(col)
        for col in key_columns if col in df2.columns
    ])
    
    return df1, df2, df1_normalized_keys, df2_normalized_keys