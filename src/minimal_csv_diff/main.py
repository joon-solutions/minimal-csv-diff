import os
import sys
import argparse
import pandas as pd
import csv
import tempfile
import uuid
from typing import Dict, List, Any, Optional
from pathlib import Path

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
        tuple: (differences_found: bool, output_file: str, summary: dict)
        
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
        >>> found, file, summary = diff_csv('data1.csv', 'data2.csv', ',', ['id', 'date'], 'output.csv')
        >>> if found:
        ...     print(f"Differences written to {file}")
        
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
        
        # Generate summary statistics
        summary = {
            'total_differences': len(unique),
            'unique_rows': len(unique[unique['failed_columns'] == 'UNIQUE ROW']),
            'modified_rows': len(unique[unique['failed_columns'] != 'UNIQUE ROW']),
            'files_compared': [os.path.basename(file1), os.path.basename(file2)],
            'common_columns': len(common_pool),
            'key_columns_used': key_columns
        }
        
        print(f"Differences have been written to '{output_file}'")
        return True, output_file, summary
    else:
        print('No differences found.')
        summary = {
            'total_differences': 0,
            'unique_rows': 0,
            'modified_rows': 0,
            'files_compared': [os.path.basename(file1), os.path.basename(file2)],
            'common_columns': len(common_pool),
            'key_columns_used': key_columns
        }
        return False, None, summary

def compare_csv_files(file1: str, file2: str, key_columns: List[str], 
                     delimiter: str = ',', output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Programmatic CSV comparison function for LLM agents.
    
    This function provides a clean API for automated CSV comparison workflows.
    It wraps the core diff_csv functionality with structured error handling
    and standardized return format.
    
    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file  
        key_columns (List[str]): Column names to use as composite key for matching rows
        delimiter (str): CSV delimiter character (default: ',')
        output_file (Optional[str]): Output file path. If None, auto-generates unique filename
    
    Returns:
        Dict containing:
            - status (str): 'success', 'no_differences', or 'error'
            - differences_found (bool): Whether differences were detected
            - output_file (str): Path to generated diff file (None if no differences)
            - summary (Dict): Statistics about the comparison
            - error_message (str): Error details if status is 'error'
            - files_processed (List[str]): Input files that were compared
    
    Example:
        >>> result = compare_csv_files('old.csv', 'new.csv', ['id', 'date'])
        >>> if result['status'] == 'success' and result['differences_found']:
        ...     print(f"Found {result['summary']['total_differences']} differences")
        ...     # Process diff file at result['output_file']
    
    LLM Agent Workflow:
        1. Get key recommendations from eda_analyzer
        2. Call this function with recommended keys
        3. Check result['status'] for success/error
        4. Process diff file if differences found
        
    Error Handling:
        - File not found errors
        - Invalid key columns
        - CSV parsing errors
        - Permission errors
    """
    try:
        # Validate input files exist
        if not os.path.exists(file1):
            return {
                'status': 'error',
                'differences_found': False,
                'output_file': None,
                'summary': {},
                'error_message': f"File not found: {file1}",
                'files_processed': []
            }
        
        if not os.path.exists(file2):
            return {
                'status': 'error',
                'differences_found': False,
                'output_file': None,
                'summary': {},
                'error_message': f"File not found: {file2}",
                'files_processed': []
            }
        
        # Generate output filename if not provided
        if output_file is None:
            session_id = str(uuid.uuid4())[:8]
            output_file = f"diff_{session_id}.csv"
        
        # Validate key columns exist in both files
        try:
            df1_cols = pd.read_csv(file1, delimiter=delimiter, nrows=0).columns.tolist()
            df2_cols = pd.read_csv(file2, delimiter=delimiter, nrows=0).columns.tolist()
            
            missing_in_file1 = [col for col in key_columns if col not in df1_cols]
            missing_in_file2 = [col for col in key_columns if col not in df2_cols]
            
            if missing_in_file1 or missing_in_file2:
                error_msg = []
                if missing_in_file1:
                    error_msg.append(f"Key columns missing in {file1}: {missing_in_file1}")
                if missing_in_file2:
                    error_msg.append(f"Key columns missing in {file2}: {missing_in_file2}")
                
                return {
                    'status': 'error',
                    'differences_found': False,
                    'output_file': None,
                    'summary': {},
                    'error_message': '; '.join(error_msg),
                    'files_processed': [file1, file2]
                }
        
        except Exception as e:
            return {
                'status': 'error',
                'differences_found': False,
                'output_file': None,
                'summary': {},
                'error_message': f"Error reading CSV headers: {str(e)}",
                'files_processed': [file1, file2]
            }
        
        # Perform the comparison
        differences_found, result_file, summary = diff_csv(
            file1, file2, delimiter, key_columns, output_file
        )
        
        if differences_found:
            return {
                'status': 'success',
                'differences_found': True,
                'output_file': result_file,
                'summary': summary,
                'error_message': None,
                'files_processed': [file1, file2]
            }
        else:
            return {
                'status': 'no_differences',
                'differences_found': False,
                'output_file': None,
                'summary': summary,
                'error_message': None,
                'files_processed': [file1, file2]
            }
            
    except Exception as e:
        return {
            'status': 'error',
            'differences_found': False,
            'output_file': None,
            'summary': {},
            'error_message': str(e),
            'files_processed': [file1, file2]
        }

def quick_csv_diff(file1: str, file2: str, auto_detect_keys: bool = True, 
                   delimiter: str = ',', output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    One-function CSV diff with automatic key detection for LLM agents.
    
    This is the highest-level function that combines EDA analysis and CSV comparison
    in a single call. Ideal for LLM agents that want a simple interface.
    
    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file
        auto_detect_keys (bool): Whether to automatically detect key columns (default: True)
        delimiter (str): CSV delimiter character (default: ',')
        output_file (Optional[str]): Output file path. If None, auto-generates unique filename
    
    Returns:
        Dict containing all fields from compare_csv_files() plus:
            - key_detection (Dict): Information about key detection process
            - recommended_keys (List[str]): Keys that were automatically detected
            - key_confidence (float): Confidence score for key detection
    
    Example:
        >>> result = quick_csv_diff('data_v1.csv', 'data_v2.csv')
        >>> if result['status'] == 'success':
        ...     print(f"Used keys: {result['recommended_keys']}")
        ...     print(f"Confidence: {result['key_confidence']:.1f}%")
        ...     if result['differences_found']:
        ...         print(f"Diff saved to: {result['output_file']}")
    
    LLM Agent Usage:
        This is the recommended entry point for most LLM agent workflows:
        
        >>> # Simple case - let the function handle everything
        >>> result = quick_csv_diff('old_data.csv', 'new_data.csv')
        >>> 
        >>> # Check results
        >>> if result['status'] == 'success':
        ...     if result['differences_found']:
        ...         # Process the diff file
        ...         diff_data = pd.read_csv(result['output_file'])
        ...     else:
        ...         print("Files are identical")
        >>> else:
        ...     print(f"Error: {result['error_message']}")
    
    Workflow:
        1. Automatically detect optimal key columns using EDA analysis
        2. Validate key columns exist in both files
        3. Perform CSV comparison using detected keys
        4. Return comprehensive results with metadata
    """
    try:
        # Import here to avoid circular imports
        try:
            from .eda_analyzer import get_recommended_keys
        except ImportError:
            # Fallback for direct execution
            from eda_analyzer import get_recommended_keys
        
        recommended_keys = []
        key_confidence = 0
        key_detection_info = {}
        
        if auto_detect_keys:
            # Get key recommendations from EDA analyzer
            key_result = get_recommended_keys([file1, file2], delimiter)
            
            if key_result['status'] == 'success' and key_result['recommended_keys']:
                recommended_keys = key_result['recommended_keys']
                key_confidence = key_result['key_confidence']
                key_detection_info = {
                    'method': 'automatic',
                    'key_type': key_result['key_type'],
                    'analysis_summary': key_result['analysis_summary']
                }
            else:
                # Fallback: try to find common columns as keys
                try:
                    df1_cols = pd.read_csv(file1, delimiter=delimiter, nrows=0).columns.tolist()
                    df2_cols = pd.read_csv(file2, delimiter=delimiter, nrows=0).columns.tolist()
                    common_cols = list(set(df1_cols) & set(df2_cols))
                    
                    if common_cols:
                        # Use first common column as fallback
                        recommended_keys = [common_cols[0]]
                        key_confidence = 50.0  # Low confidence fallback
                        key_detection_info = {
                            'method': 'fallback',
                            'key_type': 'single_fallback',
                            'available_columns': common_cols,
                            'warning': 'Used first common column as fallback key'
                        }
                    else:
                        return {
                            'status': 'error',
                            'differences_found': False,
                            'output_file': None,
                            'summary': {},
                            'error_message': 'No common columns found between files',
                            'files_processed': [file1, file2],
                            'key_detection': {'method': 'failed', 'error': 'No common columns'},
                            'recommended_keys': [],
                            'key_confidence': 0
                        }
                        
                except Exception as e:
                    return {
                        'status': 'error',
                        'differences_found': False,
                        'output_file': None,
                        'summary': {},
                        'error_message': f'Key detection failed: {str(e)}',
                        'files_processed': [file1, file2],
                        'key_detection': {'method': 'failed', 'error': str(e)},
                        'recommended_keys': [],
                        'key_confidence': 0
                    }
        else:
            return {
                'status': 'error',
                'differences_found': False,
                'output_file': None,
                'summary': {},
                'error_message': 'auto_detect_keys is False but no keys provided. Use compare_csv_files() instead.',
                'files_processed': [file1, file2],
                'key_detection': {'method': 'disabled'},
                'recommended_keys': [],
                'key_confidence': 0
            }
        
        # Perform the comparison using detected keys
        comparison_result = compare_csv_files(
            file1, file2, recommended_keys, delimiter, output_file
        )
        
        # Add key detection information to the result
        comparison_result.update({
            'key_detection': key_detection_info,
            'recommended_keys': recommended_keys,
            'key_confidence': key_confidence
        })
        
        return comparison_result
        
    except Exception as e:
        return {
            'status': 'error',
            'differences_found': False,
            'output_file': None,
            'summary': {},
            'error_message': f'Unexpected error in quick_csv_diff: {str(e)}',
            'files_processed': [file1, file2],
            'key_detection': {'method': 'failed', 'error': str(e)},
            'recommended_keys': [],
            'key_confidence': 0
        }

def batch_csv_diff(file_pairs: List[tuple], delimiter: str = ',', 
                   output_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Compare multiple pairs of CSV files in batch for LLM agents.
    
    Useful for processing multiple file comparisons in a single operation.
    Each pair is processed independently with automatic key detection.
    
    Args:
        file_pairs (List[tuple]): List of (file1, file2) tuples to compare
        delimiter (str): CSV delimiter character (default: ',')
        output_dir (Optional[str]): Directory for output files. If None, uses current directory
    
    Returns:
        Dict containing:
            - status (str): 'success', 'partial_success', or 'error'
            - results (List[Dict]): Individual results for each file pair
            - summary (Dict): Overall statistics
            - output_directory (str): Directory containing diff files
            - error_message (str): Error details if status is 'error'
    
    Example:
        >>> pairs = [('old1.csv', 'new1.csv'), ('old2.csv', 'new2.csv')]
        >>> result = batch_csv_diff(pairs)
        >>> for i, pair_result in enumerate(result['results']):
        ...     print(f"Pair {i+1}: {pair_result['status']}")
    
    LLM Agent Usage:
        >>> # Process multiple file pairs
        >>> file_pairs = [
        ...     ('data_jan.csv', 'data_jan_updated.csv'),
        ...     ('data_feb.csv', 'data_feb_updated.csv')
        ... ]
        >>> batch_result = batch_csv_diff(file_pairs)
        >>> 
        >>> # Check overall status
        >>> if batch_result['status'] in ['success', 'partial_success']:
        ...     for result in batch_result['results']:
        ...         if result['differences_found']:
        ...             print(f"Differences in {result['files_processed']}")
    """
    try:
        if output_dir is None:
            output_dir = os.getcwd()
        elif not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        results = []
        successful_comparisons = 0
        total_differences_found = 0
        
        for i, (file1, file2) in enumerate(file_pairs):
            # Generate unique output filename for this pair
            session_id = str(uuid.uuid4())[:8]
            pair_output = os.path.join(output_dir, f"diff_pair_{i+1}_{session_id}.csv")
            
            # Run comparison for this pair
            pair_result = quick_csv_diff(file1, file2, output_file=pair_output, delimiter=delimiter)
            
            # Add pair metadata
            pair_result['pair_index'] = i + 1
            pair_result['pair_files'] = (file1, file2)
            
            results.append(pair_result)
            
            if pair_result['status'] in ['success', 'no_differences']:
                successful_comparisons += 1
                if pair_result['differences_found']:
                    total_differences_found += 1
        
        # Determine overall status
        if successful_comparisons == len(file_pairs):
            overall_status = 'success'
        elif successful_comparisons > 0:
            overall_status = 'partial_success'
        else:
            overall_status = 'error'
        
        summary = {
            'total_pairs': len(file_pairs),
            'successful_comparisons': successful_comparisons,
            'failed_comparisons': len(file_pairs) - successful_comparisons,
            'pairs_with_differences': total_differences_found,
            'pairs_identical': successful_comparisons - total_differences_found
        }
        
        return {
            'status': overall_status,
            'results': results,
            'summary': summary,
            'output_directory': output_dir,
            'error_message': None if overall_status != 'error' else 'All comparisons failed'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'results': [],
            'summary': {},
            'output_directory': output_dir if 'output_dir' in locals() else None,
            'error_message': f'Batch processing error: {str(e)}'
        }

def get_diff_summary(diff_file: str) -> Dict[str, Any]:
    """
    Parse and summarize a diff file generated by compare_csv_files().
    
    Useful for LLM agents to quickly understand the contents of a diff file
    without having to parse the CSV manually.
    
    Args:
        diff_file (str): Path to the diff CSV file
    
    Returns:
        Dict containing:
            - status (str): 'success' or 'error'
            - total_rows (int): Total number of difference rows
            - unique_rows (int): Rows that exist in only one file
            - modified_rows (int): Rows that exist in both files but with differences
            - files_compared (List[str]): Names of the original files compared
            - affected_columns (List[str]): Columns that have differences
            - sample_differences (List[Dict]): Sample of actual differences
            - error_message (str): Error details if status is 'error'
    
    Example:
        >>> summary = get_diff_summary('diff_12345.csv')
        >>> if summary['status'] == 'success':
        ...     print(f"Found {summary['total_rows']} differences")
        ...     print(f"Affected columns: {summary['affected_columns']}")
    
    LLM Agent Usage:
        >>> # After running comparison
        >>> result = quick_csv_diff('old.csv', 'new.csv')
        >>> if result['differences_found']:
        ...     summary = get_diff_summary(result['output_file'])
        ...     # Analyze the summary for insights
    """
    try:
        if not os.path.exists(diff_file):
            return {
                'status': 'error',
                'error_message': f'Diff file not found: {diff_file}',
                'total_rows': 0,
                'unique_rows': 0,
                'modified_rows': 0,
                'files_compared': [],
                'affected_columns': [],
                'sample_differences': []
            }
        
        # Read the diff file
        diff_df = pd.read_csv(diff_file)
        
        if len(diff_df) == 0:
            return {
                'status': 'success',
                'total_rows': 0,
                'unique_rows': 0,
                'modified_rows': 0,
                'files_compared': [],
                'affected_columns': [],
                'sample_differences': []
            }
        
        # Analyze the diff content
        total_rows = len(diff_df)
        unique_rows = len(diff_df[diff_df['failed_columns'] == 'UNIQUE ROW'])
        modified_rows = total_rows - unique_rows
        
        # Get files that were compared
        files_compared = diff_df['source'].unique().tolist()
        
        # Extract affected columns from failed_columns
        affected_columns = set()
        for failed_cols in diff_df['failed_columns'].dropna():
            if failed_cols != 'UNIQUE ROW' and failed_cols:
                cols = failed_cols.split('| - |')
                affected_columns.update(cols)
        affected_columns = list(affected_columns)
        
        # Get sample differences (first 5 rows)
        sample_differences = []
        for _, row in diff_df.head(5).iterrows():
            sample_diff = {
                'surrogate_key': row.get('surrogate_key', ''),
                'source': row.get('source', ''),
                'failed_columns': row.get('failed_columns', ''),
                'type': 'unique' if row.get('failed_columns') == 'UNIQUE ROW' else 'modified'
            }
            sample_differences.append(sample_diff)
        
        return {
            'status': 'success',
            'total_rows': total_rows,
            'unique_rows': unique_rows,
            'modified_rows': modified_rows,
            'files_compared': files_compared,
            'affected_columns': affected_columns,
            'sample_differences': sample_differences,
            'error_message': None
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error_message': f'Error parsing diff file: {str(e)}',
            'total_rows': 0,
            'unique_rows': 0,
            'modified_rows': 0,
            'files_compared': [],
            'affected_columns': [],
            'sample_differences': []
        }

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

# LLM AGENT CONVENIENCE FUNCTIONS

def analyze_and_diff(file1: str, file2: str, delimiter: str = ',') -> Dict[str, Any]:
    """
    Convenience function that combines EDA analysis and diff in one call.
    
    This is an alias for quick_csv_diff() with a more descriptive name.
    Recommended for LLM agents who want the simplest possible interface.
    
    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        Dict: Same as quick_csv_diff() return format
    
    Example:
        >>> result = analyze_and_diff('before.csv', 'after.csv')
        >>> if result['differences_found']:
        ...     print(f"Changes detected in: {result['recommended_keys']}")
    """
    return quick_csv_diff(file1, file2, delimiter=delimiter)

def simple_csv_compare(file1: str, file2: str, keys: List[str], delimiter: str = ',') -> bool:
    """
    Simplified function that returns just True/False for differences.
    
    Useful for LLM agents that only need to know if files are different,
    without needing detailed analysis.
    
    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file
        keys (List[str]): Key columns to use for comparison
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        bool: True if differences found, False if files are identical
    
    Example:
        >>> has_changes = simple_csv_compare('old.csv', 'new.csv', ['id'])
        >>> if has_changes:
        ...     print("Files have differences")
        >>> else:
        ...     print("Files are identical")
    """
    try:
        result = compare_csv_files(file1, file2, keys, delimiter)
        return result.get('differences_found', False)
    except:
        return False

def get_file_columns(file_path: str, delimiter: str = ',') -> List[str]:
    """
    Get column names from a CSV file for LLM agents.
    
    Utility function to help LLM agents discover available columns
    before running comparisons.
    
    Args:
        file_path (str): Path to CSV file
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        List[str]: Column names from the CSV file
    
    Example:
        >>> columns = get_file_columns('data.csv')
        >>> print(f"Available columns: {columns}")
    """
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, nrows=0)
        return df.columns.tolist()
    except:
        return []

def validate_key_columns(file1: str, file2: str, key_columns: List[str], 
                        delimiter: str = ',') -> Dict[str, Any]:
    """
    Validate that key columns exist in both files before running comparison.
    
    Useful for LLM agents to check key validity before attempting comparison.
    
    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file
        key_columns (List[str]): Key columns to validate
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        Dict containing:
            - valid (bool): Whether all key columns exist in both files
            - missing_in_file1 (List[str]): Keys missing from first file
            - missing_in_file2 (List[str]): Keys missing from second file
            - common_columns (List[str]): Columns present in both files
            - error_message (str): Error details if validation fails
    
    Example:
        >>> validation = validate_key_columns('f1.csv', 'f2.csv', ['id', 'date'])
        >>> if validation['valid']:
        ...     # Proceed with comparison
        ...     result = compare_csv_files('f1.csv', 'f2.csv', ['id', 'date'])
        >>> else:
        ...     print(f"Invalid keys: {validation['error_message']}")
    """
    try:
        cols1 = get_file_columns(file1, delimiter)
        cols2 = get_file_columns(file2, delimiter)
        
        if not cols1 or not cols2:
            return {
                'valid': False,
                'missing_in_file1': [],
                'missing_in_file2': [],
                'common_columns': [],
                'error_message': 'Could not read column headers from one or both files'
            }
        
        missing_in_file1 = [col for col in key_columns if col not in cols1]
        missing_in_file2 = [col for col in key_columns if col not in cols2]
        common_columns = list(set(cols1) & set(cols2))
        
        valid = len(missing_in_file1) == 0 and len(missing_in_file2) == 0
        
        error_message = None
        if not valid:
            error_parts = []
            if missing_in_file1:
                error_parts.append(f"Missing in {file1}: {missing_in_file1}")
            if missing_in_file2:
                error_parts.append(f"Missing in {file2}: {missing_in_file2}")
            error_message = "; ".join(error_parts)
        
        return {
            'valid': valid,
            'missing_in_file1': missing_in_file1,
            'missing_in_file2': missing_in_file2,
            'common_columns': common_columns,
            'error_message': error_message
        }
        
    except Exception as e:
        return {
            'valid': False,
            'missing_in_file1': [],
            'missing_in_file2': [],
            'common_columns': [],
            'error_message': f'Validation error: {str(e)}'
        }

if __name__ == "__main__":
    main()
