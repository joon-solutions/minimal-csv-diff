import os
import uuid
from typing import Dict, List, Any, Optional
import polars as pl
from .diff_engine import diff_csv_core
from .csv_processor import load_and_normalize_dfs, normalize_string

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
            df1_cols = pl.read_csv(file1, separator=delimiter, n_rows=0).columns
            df2_cols = pl.read_csv(file2, separator=delimiter, n_rows=0).columns
            
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
        
        # Load and normalize dataframes
        df1_raw, df2_raw, df1_normalized_keys, df2_normalized_keys = load_and_normalize_dfs(file1, file2, delimiter, key_columns)

        # Perform the comparison
        differences_found, result_file, summary = diff_csv_core(
            df1_raw, df2_raw, os.path.basename(file1), os.path.basename(file2), delimiter, key_columns, output_file
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
        ...         diff_data = pl.read_csv(result['output_file'])
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
        # Import here to avoid circular imports. This will be updated to direct import from eda_analyzer
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
                    df1_cols = pl.read_csv(file1, separator=delimiter, n_rows=0).columns
                    df2_cols = pl.read_csv(file2, separator=delimiter, n_rows=0).columns
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
        
        for i, (file1_path, file2_path) in enumerate(file_pairs):
            # Generate unique output filename for this pair
            session_id = str(uuid.uuid4())[:8]
            pair_output = os.path.join(output_dir, f"diff_pair_{i+1}_{session_id}.csv")
            
            # Run comparison for this pair
            pair_result = quick_csv_diff(file1_path, file2_path, output_file=pair_output, delimiter=delimiter)
            
            # Add pair metadata
            pair_result['pair_index'] = i + 1
            pair_result['pair_files'] = (file1_path, file2_path)
            
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
        diff_df = pl.read_csv(diff_file)
        
        if diff_df.is_empty():
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
        total_rows = diff_df.height
        unique_rows = diff_df.filter(pl.col('failed_columns') == 'UNIQUE ROW').height
        modified_rows = total_rows - unique_rows
        
        # Get files that were compared
        files_compared = diff_df.select('source').unique().to_series().to_list()
        
        # Extract affected columns from failed_columns
        affected_columns = set()
        for failed_cols in diff_df.select('failed_columns').drop_nulls().to_series().to_list():
            if failed_cols != 'UNIQUE ROW' and failed_cols:
                cols = failed_cols.split('| - |')
                affected_columns.update(cols)
        affected_columns = list(affected_columns)
        
        # Get sample differences (first 5 rows)
        sample_differences = []
        for row_tuple in diff_df.head(5).iter_rows():
            row_dict = dict(zip(diff_df.columns, row_tuple))
            sample_diff = {
                'surrogate_key': row_dict.get('surrogate_key', ''),
                'source': row_dict.get('source', ''),
                'failed_columns': row_dict.get('failed_columns', ''),
                'type': 'unique' if row_dict.get('failed_columns') == 'UNIQUE ROW' else 'modified'
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
        ...     else:
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
        df = pl.read_csv(file_path, separator=delimiter, n_rows=0)
        return df.columns
    except Exception:
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