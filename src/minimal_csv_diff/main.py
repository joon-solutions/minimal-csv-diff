import os
import sys
import argparse
import pandas as pd
import csv

def diff_csv(file1, file2, delimiter, key_columns, output_file='diff.csv'):
    """
    Compare two CSV files and generate a diff report showing differences.
    
    This function performs a comprehensive comparison of two CSV files, identifying
    rows that exist in one file but not the other, and highlighting specific column
    differences for rows with matching keys but different values.
    
    Args:
        file1 (str): Path to the first CSV file to compare
        file2 (str): Path to the second CSV file to compare  
        delimiter (str): CSV delimiter character (e.g., ',', ';', '\t')
        key_columns (list): List of column names to use as composite key for matching rows
        output_file (str, optional): Path for the output diff file. Defaults to 'diff.csv'
    
    Returns:
        None: Writes results to output_file and prints status message
        
    Output File Structure:
        - surrogate_key: Concatenated key columns for row identification
        - source: Filename indicating which file the row came from
        - failed_columns: Pipe-separated list of columns that differ between files
        - [original columns]: All common columns from both input files
    
    Behavior:
        - Only compares columns that exist in both files
        - Treats empty strings as None for comparison
        - Converts all data to strings to avoid type comparison issues
        - Groups differences by surrogate key to show related changes
        - Marks unique rows (exist in only one file) as 'UNIQUE ROW'
        - For matching keys with different values, lists the differing columns
    
    Example:
        >>> diff_csv('data1.csv', 'data2.csv', ',', ['id', 'date'], 'output.csv')
        Differences have been written to 'output.csv'
        
    AI Agent Usage:
        1. Use eda_analyzer to identify optimal key_columns
        2. Ensure both files exist and are readable
        3. Call this function with recommended keys
        4. Parse output file for detailed difference analysis
        
    Raises:
        FileNotFoundError: If input files don't exist
        pandas.errors.EmptyDataError: If CSV files are empty or malformed
        KeyError: If specified key_columns don't exist in both files
    """
    # Load dataframes
    df1 = pd.read_csv(file1, delimiter=delimiter, converters={i: str for i in range(100)}, quoting=csv.QUOTE_MINIMAL)
    df2 = pd.read_csv(file2, delimiter=delimiter, converters={i: str for i in range(100)}, quoting=csv.QUOTE_MINIMAL)
    df1.replace('', None, inplace=True)
    df2.replace('', None, inplace=True)

    # Build the column pool
    column_pool = list(set(df1.columns).union(df2.columns))

    # Build the common pool (columns present in both)
    common_pool = [col for col in column_pool if col in df1.columns and col in df2.columns]

    # Only keep columns in common_pool
    df1_common = df1[common_pool]
    df2_common = df2[common_pool]

    # Merge the two DataFrames
    merged_df = pd.merge(df1_common, df2_common, indicator=True, how='outer')

    # Select rows present in df1 but not in df2, and vice versa
    unique = merged_df[(merged_df['_merge'] == 'left_only') | (merged_df['_merge'] == 'right_only')]

    def flag_column(value):
        if value == 'left_only':
            return os.path.basename(file1)
        elif value == 'right_only':
            return os.path.basename(file2)
        else:
            return 'both?'

    if unique.shape[0] > 0:
        pd.options.mode.chained_assignment = None
        unique['source'] = unique['_merge'].apply(flag_column)
        unique['result'] = unique['_merge']

        # Reorder columns
        columns = ['source', 'result'] + [col for col in unique.columns if col not in ['source', 'result']]
        unique = unique[columns]

        # Create new surrogate_key column
        unique['surrogate_key'] = unique[key_columns].fillna('').astype(str).agg(''.join, axis=1)

        # Move the new column to the first position
        columns = ['surrogate_key'] + [col for col in unique.columns if col != 'surrogate_key']
        unique = unique[columns]

        # Drop 'result' and '_merge' columns
        unique.drop(columns=['result', '_merge'], inplace=True)

        # Order DataFrame by the new surrogate_key column
        unique = unique.sort_values(by='surrogate_key').reset_index(drop=True)

        # Initialize the fail_column with empty values
        unique['fail_column'] = ''

        # Count occurrences of each surrogate_key
        surrogate_counts = unique['surrogate_key'].value_counts()

        # Case 1: Identify UNIQUE ROWS (surrogate_key appears only once)
        unique_keys = surrogate_counts[surrogate_counts == 1].index
        unique.loc[unique['surrogate_key'].isin(unique_keys), 'fail_column'] = 'UNIQUE ROW'

        # Case 2: For rows with same surrogate_key but different sources, find differing columns
        for key in surrogate_counts[surrogate_counts > 1].index:
            key_rows = unique[unique['surrogate_key'] == key]
            if len(key_rows) == 2:
                row1_idx = key_rows.index[0]
                row2_idx = key_rows.index[1]
                data_columns = [col for col in unique.columns if col not in ['surrogate_key', 'source', 'fail_column']]
                differing_columns = []
                for col in data_columns:
                    if unique.loc[row1_idx, col] != unique.loc[row2_idx, col]:
                        differing_columns.append(col)
                if differing_columns:
                    fail_text = '| - |'.join(differing_columns)
                    unique.loc[row1_idx, 'fail_column'] = fail_text
                    unique.loc[row2_idx, 'fail_column'] = fail_text

        # Create a new field holding fail_column values and reorder columns
        unique.insert(2, 'failed_columns', unique['fail_column'])
        unique.drop(columns=['fail_column'], inplace=True)

        # Export the CSV
        unique.to_csv(output_file, sep=',', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
        print(f"Differences have been written to '{output_file}'")
    else:
        print('No differences found.')

def interactive_mode():
    """
    Run the CSV diff tool in interactive mode with user prompts.
    
    This function provides a guided interface for users to:
    1. Select working directory
    2. Choose CSV delimiter
    3. Pick two files to compare from available CSV files
    4. Select key columns from common columns
    5. Specify output filename
    
    The function automatically discovers CSV files in the working directory
    and presents common columns from both selected files for key selection.
    
    Returns:
        None: Calls diff_csv() with user-selected parameters
        
    AI Agent Usage:
        Not recommended for AI agents - use diff_csv() directly instead.
        This function requires interactive input and is designed for human users.
        
    Raises:
        SystemExit: If invalid input is provided or files cannot be loaded
        FileNotFoundError: If working directory doesn't exist
        pandas.errors.EmptyDataError: If selected CSV files are malformed
    """
    workdir = os.getcwd()
    diff_workdir = input(f'Workdir is "{workdir}".\nEnter to confirm or input the full path to the directory containing the CSV files to compare: \n> ')
    if diff_workdir.strip():
        workdir = diff_workdir

    os.chdir(workdir)
    print(f'Current workdir is: {workdir}')
    delimiter = input('Input the file delimiter (default: ,): \n> ') or ','

    # Get all CSV files except 'combined.csv'
    all_files = os.listdir(workdir)
    csv_files = [f for f in all_files if f.endswith('.csv') and f != 'combined.csv']

    print("Available CSV files:")
    for idx, file in enumerate(csv_files):
        print(f"{idx}: {file}")

    try:
        indices_input = input("Enter the indices of the two files to compare, separated by a comma: \n> ")
        indices = [int(idx.strip()) for idx in indices_input.split(',')]
        if len(indices) != 2:
            raise ValueError("You must provide exactly two indices.")
        file1_index, file2_index = indices
        if (file1_index not in range(len(csv_files)) or
            file2_index not in range(len(csv_files)) or
            file1_index == file2_index):
            raise ValueError("Invalid indices or indices are the same.")
    except ValueError as e:
        print(f"Invalid input: {e}")
        raise SystemExit

    csv_file1 = csv_files[file1_index]
    csv_file2 = csv_files[file2_index]

    # Load both CSVs to get columns
    df1 = pd.read_csv(csv_file1, delimiter=delimiter, converters={i: str for i in range(100)}, quoting=csv.QUOTE_MINIMAL)
    df2 = pd.read_csv(csv_file2, delimiter=delimiter, converters={i: str for i in range(100)}, quoting=csv.QUOTE_MINIMAL)
    common_columns = list(set(df1.columns) & set(df2.columns))
    print("Available columns for key selection:")
    for col in common_columns:
        print(f"- {col}")
    key_columns_input = input("Enter comma-separated column names to use as key: \n> ")
    key_columns = [col.strip() for col in key_columns_input.split(",") if col.strip() in common_columns]

    if not key_columns:
        print("No valid key columns selected.")
        raise SystemExit

    output_file = input("Enter output file name (default: diff.csv): \n> ") or "diff.csv"
    diff_csv(csv_file1, csv_file2, delimiter, key_columns, output_file)

def main():
    """
    Main entry point for the CSV diff tool supporting both CLI and interactive modes.
    
    Command Line Interface:
        python main.py file1.csv file2.csv --key "col1,col2" [options]
        
    Interactive Mode:
        python main.py (without required arguments)
    
    CLI Arguments:
        file1 (str): First CSV file path
        file2 (str): Second CSV file path
        --delimiter (str): CSV delimiter (default: ',')
        --key (str): Comma-separated key column names (required for CLI mode)
        --output (str): Output file path (default: 'diff.csv')
    
    AI Agent Usage:
        Recommended approach:
        1. Run eda_analyzer.py first to get key recommendations
        2. Use CLI mode with discovered parameters:
           subprocess.run([
               'python', 'main.py', 'file1.csv', 'file2.csv',
               '--key', 'recommended_keys',
               '--delimiter', 'detected_delimiter',
               '--output', 'diff_output.csv'
           ])
        3. Parse the generated diff file for analysis results
        
    Example CLI Usage:
        python main.py data1.csv data2.csv --key "id,date" --output results.csv
        
    Returns:
        None: Exits with status code 0 on success, 1 on error
    """
    parser = argparse.ArgumentParser(description="Diff two CSV files.")
    parser.add_argument("file1", nargs='?', help="First CSV file")
    parser.add_argument("file2", nargs='?', help="Second CSV file")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ',')")
    parser.add_argument("--key", help="Comma-separated list of column names to use as key")
    parser.add_argument("--output", default="diff.csv", help="Output CSV file (default: diff.csv)")
    args = parser.parse_args()

    # If both files and key are provided, run in CLI mode
    if args.file1 and args.file2 and args.key:
        key_columns = [col.strip() for col in args.key.split(",")]
        diff_csv(args.file1, args.file2, delimiter=args.delimiter, key_columns=key_columns, output_file=args.output)
    else:
        # Otherwise, fall back to interactive mode
        interactive_mode()

if __name__ == "__main__":
    main()