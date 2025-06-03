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
    file1 = os.path.join(os.path.dirname(__file__), '../demo/file1.csv')
    file2 = os.path.join(os.path.dirname(__file__), '../demo/file2.csv')
    expected_output = os.path.join(os.path.dirname(__file__), '../demo/diff.csv')

    # Use a temp file for output
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        output_file = tmp.name

    # Run the diff - now returns tuple
    differences_found, result_file, summary = diff_csv(
        file1, file2, delimiter=',', key_columns=['id'], output_file=output_file
    )

    # Verify new return format
    assert isinstance(differences_found, bool)
    assert isinstance(summary, dict)
    assert 'total_differences' in summary

    if differences_found:
        # Compare output to expected
        df_actual = pd.read_csv(output_file)
        df_expected = pd.read_csv(expected_output)

        # Sort by surrogate_key for reliable comparison
        df_actual = df_actual.sort_values(by='surrogate_key').reset_index(drop=True)
        df_expected = df_expected.sort_values(by='surrogate_key').reset_index(drop=True)

        # Reorder columns to match expected
        df_actual = df_actual[df_expected.columns]

        # Compare DataFrames
        pd.testing.assert_frame_equal(df_actual, df_expected)

    # Clean up temp file
    os.remove(output_file)

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
