import os
import polars as pl
import tempfile
import pytest
from unittest.mock import patch
from src.minimal_csv_diff.api import (
    compare_csv_files,
    quick_csv_diff,
    simple_csv_compare,
    get_file_columns,
    validate_key_columns
)
from src.minimal_csv_diff.diff_engine import diff_csv_core

def test_diff_csv_produces_expected_output():
    # Paths to demo files
    file1 = os.path.join(os.path.dirname(__file__), '../demo/bug_legacy.csv')
    file2 = os.path.join(os.path.dirname(__file__), '../demo/bug_new.csv')
    
    # Use a temp file for output
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        output_file = tmp.name

    # Run the diff using the new API
    result = compare_csv_files(
        file1, file2, key_columns=['id', 'dimension_name', 'view_name'], delimiter=',', output_file=output_file
    )
    differences_found = result['differences_found']
    result_file = result['output_file']
    summary = result['summary']

    assert differences_found, "Expected differences to be found"
    assert result_file is not None, "Output file path should not be None"
    assert os.path.exists(result_file), f"Output file {result_file} was not created"

    diff_df = pl.read_csv(result_file)

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
    for row in diff_df.iter_rows(named=True):
        assert 'sql_definition' in row['failed_columns'], f"Row did not show 'sql_definition' as a failed column: {row['failed_columns']}"
        assert row['failed_columns'] != 'UNIQUE ROW', f"Row was incorrectly marked as 'UNIQUE ROW'"

    # Verify the sources for the modified rows
    modified_rows_sources = diff_df['source'].to_list()
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
        df = pl.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.write_csv(f.name, include_header=True)
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
        df = pl.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.write_csv(f.name, include_header=True)
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
        df = pl.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.write_csv(f.name, include_header=True)
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
    df = pl.DataFrame(data)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.write_csv(f.name, include_header=True)
        
        try:
            columns = get_file_columns(f.name)
            assert columns == ['col1', 'col2']
        finally:
            os.unlink(f.name)

def test_validate_key_columns():
    """Test key validation utility."""
    data = {'id': [1, 2], 'name': ['A', 'B']}
    df = pl.DataFrame(data)
    
    files = []
    for _ in range(2):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.write_csv(f.name, include_header=True)
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

def test_identical_files_produce_zero_diff():
    """
    CRITICAL TEST: Comparing identical files must produce zero differences.
    
    This catches the bug where NULL key columns caused false positive 'unique rows'
    because Polars anti-joins treat NULL != NULL.
    """
    # Create test data with some NULL/empty key columns (the bug trigger)
    data = {
        'project': ['acme-prod', 'acme-prod', 'acme-prod'],
        'explore_name': ['', '', 'sales_explore'],  # Empty strings -> NULL after processing
        'view_name': ['user_attrs', 'user_attrs', 'orders'],
        'field_name': ['city', 'country', 'total'],
        'model_name': ['', '', 'sales_model'],  # Empty strings -> NULL after processing
        'field_type': ['dimension', 'dimension', 'measure'],
        'sql': ['${TABLE}.city', '${TABLE}.country', 'SUM(${TABLE}.amount)'],
    }
    
    df = pl.DataFrame(data)
    
    # Write same data to two temp files
    files = []
    for _ in range(2):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.write_csv(f.name, include_header=True)
            files.append(f.name)
    
    try:
        result = compare_csv_files(
            files[0], files[1], 
            key_columns=['project', 'explore_name', 'view_name', 'field_name', 'model_name']
        )
        
        # CRITICAL ASSERTIONS
        assert result['status'] == 'no_differences', f"Expected 'no_differences', got '{result['status']}'"
        assert result['differences_found'] == False, "Identical files should have no differences"
        assert result['summary']['total_differences'] == 0, f"Expected 0 differences, got {result['summary']['total_differences']}"
        assert result['summary']['unique_rows'] == 0, f"Expected 0 unique rows, got {result['summary']['unique_rows']}"
        
    finally:
        for f in files:
            os.unlink(f)


def test_null_key_columns_match_correctly():
    """
    Test that rows with NULL/empty key columns still match correctly between files.
    
    This specifically tests the normalization pipeline where:
    1. Empty strings are converted to NULL in load_and_normalize_dfs
    2. NULL is then converted back to "" by normalize_column_expr for joining
    """
    # File 1: Has rows with empty key columns
    data1 = {
        'project': ['acme-prod', 'acme-prod'],
        'explore_name': ['', 'sales'],  # First row has empty explore_name
        'view_name': ['user_attrs', 'orders'],
        'field_name': ['city', 'total'],
        'model_name': ['', 'sales_model'],  # First row has empty model_name
        'value': ['A', 'B'],
    }
    
    # File 2: Same structure, but 'value' differs for second row
    data2 = {
        'project': ['acme-prod', 'acme-prod'],
        'explore_name': ['', 'sales'],
        'view_name': ['user_attrs', 'orders'],
        'field_name': ['city', 'total'],
        'model_name': ['', 'sales_model'],
        'value': ['A', 'CHANGED'],  # Only this should show as modified
    }
    
    files = []
    for data in [data1, data2]:
        df = pl.DataFrame(data)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.write_csv(f.name, include_header=True)
            files.append(f.name)
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            output_file = tmp.name
        
        result = compare_csv_files(
            files[0], files[1],
            key_columns=['project', 'explore_name', 'view_name', 'field_name', 'model_name'],
            output_file=output_file
        )
        
        assert result['status'] == 'success', f"Expected 'success', got '{result['status']}'"
        assert result['differences_found'] == True, "Should find the modified row"
        
        # Should only find the MODIFIED row (sales/orders/total), not false positive unique rows
        assert result['summary']['unique_rows'] == 0, f"Expected 0 unique rows (no false positives), got {result['summary']['unique_rows']}"
        assert result['summary']['modified_rows'] == 2, f"Expected 2 modified rows (one per file), got {result['summary']['modified_rows']}"
        
        # Verify the diff content
        diff_df = pl.read_csv(output_file)
        assert len(diff_df) == 2, f"Expected 2 rows in diff (modified row from each file), got {len(diff_df)}"
        
        # Both rows should be about the 'sales' explore, not the empty-key row
        for row in diff_df.iter_rows(named=True):
            assert row['explore_name'] == 'sales', f"Wrong row in diff: explore_name={row['explore_name']}"
            assert 'value' in row['failed_columns'], f"Expected 'value' in failed_columns, got {row['failed_columns']}"
        
        if os.path.exists(output_file):
            os.unlink(output_file)
            
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

    result = compare_csv_files(
        file1_path, file2_path, key_columns=key_columns, delimiter=',', output_file=output_file
    )
    differences_found = result['differences_found']
    result_file = result['output_file']
    summary = result['summary']

    assert differences_found, "Expected differences to be found"
    assert result_file is not None, "Output file path should not be None"
    assert os.path.exists(result_file), f"Output file {result_file} was not created"

    diff_df = pl.read_csv(result_file)

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
    for row in diff_df.iter_rows(named=True):
        assert 'sql_definition' in row['failed_columns'], f"Row did not show 'sql_definition' as a failed column: {row['failed_columns']}"
        assert row['failed_columns'] != 'UNIQUE ROW', f"Row was incorrectly marked as 'UNIQUE ROW'"

    # Verify the sources for the modified rows
    modified_rows_sources = diff_df['source'].to_list()
    expected_sources = [os.path.basename(file1_path), os.path.basename(file2_path),
                        os.path.basename(file1_path), os.path.basename(file2_path)]
    assert sorted(modified_rows_sources) == sorted(expected_sources), "Incorrect sources for modified rows"

    if result_file and os.path.exists(result_file):
        os.remove(result_file)