import polars as pl
import re
from typing import Any, List

def normalize_string(s: Any) -> str:
    """
    Normalizes a string by stripping whitespace, standardizing internal spaces,
    and handling None values.
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
    df1_normalized_keys = df1.with_columns([
        pl.col(col).map_elements(normalize_string, return_dtype=pl.Utf8)
        for col in key_columns if col in df1.columns
    ])
    df2_normalized_keys = df2.with_columns([
        pl.col(col).map_elements(normalize_string, return_dtype=pl.Utf8)
        for col in key_columns if col in df2.columns
    ])
    
    return df1, df2, df1_normalized_keys, df2_normalized_keys