import os
import sys
import argparse
import polars as pl
from typing import List, Any, Dict, Optional
from .api import quick_csv_diff, compare_csv_files, get_file_columns

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
        polars.exceptions.NoDataError: If selected CSV files are empty or malformed
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
    df1_cols = get_file_columns(csv_file1, delimiter)
    df2_cols = get_file_columns(csv_file2, delimiter)
    common_columns = list(set(df1_cols) & set(df2_cols))
    print("Available columns for key selection:")
    for col in common_columns:
        print(f"- {col}")
    key_columns_input = input("Enter comma-separated column names to use as key (leave empty for auto-detection): \n> ")
    
    key_columns = [col.strip() for col in key_columns_input.split(",") if col.strip()] if key_columns_input.strip() else []

    if not key_columns:
        print("Attempting to auto-detect key columns...")
        result = quick_csv_diff(csv_file1, csv_file2, delimiter=delimiter, output_file="diff.csv")
    else:
        result = compare_csv_files(csv_file1, csv_file2, key_columns, delimiter=delimiter, output_file="diff.csv")

    if result['status'] == 'success':
        if result['differences_found']:
            print(f"Differences found and written to '{result['output_file']}'. Summary: {result['summary']}")
        else:
            print("No differences found.")
    else:
        print(f"Error during comparison: {result['error_message']}")
        if 'key_detection' in result and result['key_detection'].get('error'):
            print(f"Key detection error: {result['key_detection']['error']}")
        raise SystemExit

def main_cli():
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
    if args.file1 and args.file2:
        if args.key:
            key_columns = [col.strip() for col in args.key.split(",")]
            result = compare_csv_files(args.file1, args.file2, delimiter=args.delimiter, key_columns=key_columns, output_file=args.output)
        else:
            print("No key columns provided. Attempting to auto-detect keys.")
            result = quick_csv_diff(args.file1, args.file2, delimiter=args.delimiter, output_file=args.output)
        
        if result['status'] == 'success':
            if result['differences_found']:
                print(f"Differences found and written to '{result['output_file']}'. Summary: {result['summary']}")
            else:
                print("No differences found.")
            sys.exit(0)
        else:
            print(f"Error during comparison: {result['error_message']}")
            if 'key_detection' in result and result['key_detection'].get('error'):
                print(f"Key detection error: {result['key_detection']['error']}")
            sys.exit(1)
    else:
        # Otherwise, fall back to interactive mode
        interactive_mode()
