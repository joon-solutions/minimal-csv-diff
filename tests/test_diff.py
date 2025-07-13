import os
import pandas as pd
import tempfile
import pytest
from unittest.mock import patch
from src.minimal_csv_diff.main import (
    diff_csv, 
    compare_csv_files, 
    quick_csv_diff, 
    simple_csv_compare,
    get_file_columns,
    validate_key_columns
)

def test_diff_csv_produces_expected_output():
    # Paths to demo files
    file1 = os.path.join(os.path.dirname(__file__), '../demo/bug_legacy.csv')
    file2 = os.path.join(os.path.dirname(__file__), '../demo/bug_new.csv')
    
    # Use a temp file for output
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        output_file = tmp.name

    # Run the diff
    differences_found, result_file, summary = diff_csv(
        file1, file2, delimiter=',', key_columns=['id', 'dimension_name', 'view_name'], output_file=output_file
    )

    assert differences_found, "Expected differences to be found"
    assert result_file is not None, "Output file path should not be None"
    assert os.path.exists(result_file), f"Output file {result_file} was not created"

    diff_df = pd.read_csv(result_file)

    # Expected:
    # id=1: sql_definition changed (region filter removed) -> 2 entries
    # id=2: identical -> 0 entries
    # id=3: sql_definition changed (is_active filter removed) -> 2 entries
    # Total expected rows in diff_df: 2 + 2 = 4
    assert len(diff_df) == 4, f"Expected 4 rows in diff output, but got {len(diff_df)}"
    assert summary['total_differences'] == 4, f"Expected 4 total differences in summary, got {summary['total_differences']}"
    assert summary['unique_rows'] == 0, f"Expected 0 unique rows in summary, got {summary['unique_rows']}"
    assert summary['modified_rows'] == 4, f"Expected 4 modified rows in summary, got {summary['modified_rows']}"

    # Check that all 4 rows have 'sql_definition' in their 'failed_columns'
    for index, row in diff_df.iterrows():
        assert 'sql_definition' in row['failed_columns'], f"Row {index} did not show 'sql_definition' as a failed column: {row['failed_columns']}"
        assert row['failed_columns'] != 'UNIQUE ROW', f"Row {index} was incorrectly marked as 'UNIQUE ROW'"

    # Verify the sources for the modified rows
    modified_rows_sources = diff_df['source'].tolist()
    expected_sources = [os.path.basename(file1), os.path.basename(file2),
                        os.path.basename(file1), os.path.basename(file2)]
    assert sorted(modified_rows_sources) == sorted(expected_sources), "Incorrect sources for modified rows"

    if result_file and os.path.exists(result_file):
        os.remove(result_file)

def test_compare_csv_files():
    """Test the new programmatic API."""
    # Create test files
    data1 = {'id': [1, 2], 'name': ['A', 'B']}
    data2 = {'id': [1, 3], 'name': ['A', 'C']}  # Different
    
    files = []
    for data in [data1, data2]:
        df = pd.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            files.append(f.name)
    
    try:
        result = compare_csv_files(files[0], files[1], ['id'])
        
        # Check return structure
        assert 'status' in result
        assert 'differences_found' in result
        assert 'summary' in result
        assert result['status'] in ['success', 'no_differences', 'error']
        
        if result['output_file']:
            os.unlink(result['output_file'])
            
    finally:
        for f in files:
            os.unlink(f)

def test_quick_csv_diff():
    """Test auto-detection function."""
    # Create test files with clear ID column
    data1 = {'customer_id': [1, 2], 'name': ['A', 'B']}
    data2 = {'customer_id': [1, 3], 'name': ['A', 'C']}
    
    files = []
    for data in [data1, data2]:
        df = pd.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            files.append(f.name)
    
    try:
        # Mock the EDA analyzer - patch where it's imported
        with patch('src.minimal_csv_diff.eda_analyzer.get_recommended_keys') as mock_keys:
            mock_keys.return_value = {
                'status': 'success',
                'recommended_keys': ['customer_id'],
                'key_confidence': 95.0,
                'key_type': 'single'
            }
            
            result = quick_csv_diff(files[0], files[1])
            
            # Check extended return structure
            assert 'recommended_keys' in result
            assert 'key_confidence' in result
            assert 'key_detection' in result
            
            if result['output_file']:
                os.unlink(result['output_file'])
                
    finally:
        for f in files:
            os.unlink(f)

def test_simple_csv_compare():
    """Test boolean return function."""
    data1 = {'id': [1, 2], 'name': ['A', 'B']}
    data2 = {'id': [1, 3], 'name': ['A', 'C']}  # Different
    
    files = []
    for data in [data1, data2]:
        df = pd.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            files.append(f.name)
    
    try:
        has_diff = simple_csv_compare(files[0], files[1], ['id'])
        assert isinstance(has_diff, bool)
    finally:
        for f in files:
            os.unlink(f)

def test_get_file_columns():
    """Test column extraction utility."""
    data = {'col1': [1, 2], 'col2': ['A', 'B']}
    df = pd.DataFrame(data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f.name, index=False)
        
        try:
            columns = get_file_columns(f.name)
            assert columns == ['col1', 'col2']
        finally:
            os.unlink(f.name)

def test_validate_key_columns():
    """Test key validation utility."""
    data = {'id': [1, 2], 'name': ['A', 'B']}
    df = pd.DataFrame(data)
    
    files = []
    for _ in range(2):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            files.append(f.name)
    
    try:
        # Valid keys
        result = validate_key_columns(files[0], files[1], ['id'])
        assert result['valid'] == True
        
        # Invalid keys
        result = validate_key_columns(files[0], files[1], ['nonexistent'])
        assert result['valid'] == False
        assert len(result['missing_in_file1']) > 0
        
    finally:
        for f in files:
            os.unlink(f)

def test_complex_sql_diff_and_unique_rows():
    """
    Tests diff_csv with complex SQL content and verifies both modified and unique rows.
    Uses the sanitized demo files.
    """
    file1_path = os.path.join(os.path.dirname(__file__), '../demo/bug_legacy.csv')
    file2_path = os.path.join(os.path.dirname(__file__), '../demo/bug_new.csv')
    output_file = "test_complex_diff_output.csv"
    key_columns = ["id", "dimension_name", "view_name"]

    differences_found, result_file, summary = diff_csv(
        file1_path, file2_path, ',', key_columns, output_file
    )

    assert differences_found, "Expected differences to be found"
    assert result_file is not None, "Output file path should not be None"
    assert os.path.exists(result_file), f"Output file {result_file} was not created"

    diff_df = pd.read_csv(result_file)

    # Expected:
    # id=1: sql_definition changed (region filter removed) -> 2 entries
    # id=2: identical -> 0 entries
    # id=3: sql_definition changed (is_active filter removed) -> 2 entries
    # Total expected rows in diff_df: 2 + 2 = 4
    assert len(diff_df) == 4, f"Expected 4 rows in diff output, but got {len(diff_df)}"
    assert summary['total_differences'] == 4, f"Expected 4 total differences in summary, got {summary['total_differences']}"
    assert summary['unique_rows'] == 0, f"Expected 0 unique rows in summary, got {summary['unique_rows']}"
    assert summary['modified_rows'] == 4, f"Expected 4 modified rows in summary, got {summary['modified_rows']}"

    # Check that all 4 rows have 'sql_definition' in their 'failed_columns'
    for index, row in diff_df.iterrows():
        assert 'sql_definition' in row['failed_columns'], f"Row {index} did not show 'sql_definition' as a failed column: {row['failed_columns']}"
        assert row['failed_columns'] != 'UNIQUE ROW', f"Row {index} was incorrectly marked as 'UNIQUE ROW'"

    # Verify the sources for the modified rows
    modified_rows_sources = diff_df['source'].tolist()
    expected_sources = [os.path.basename(file1_path), os.path.basename(file2_path),
                        os.path.basename(file1_path), os.path.basename(file2_path)]
    assert sorted(modified_rows_sources) == sorted(expected_sources), "Incorrect sources for modified rows"

    if result_file and os.path.exists(result_file):
        os.remove(result_file)
