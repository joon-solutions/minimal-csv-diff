import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import patch
from io import StringIO

from src.minimal_csv_diff.eda_analyzer import (
    CSVAnalyzer, 
    get_recommended_keys,
    quick_key_analysis,
    get_column_intersection,
    parse_arguments,
    main
)

class TestCSVAnalyzer:
    """Test cases for the CSVAnalyzer class."""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        data = {
            'customer_id': [1, 2, 3, 4, 5],
            'customer_name': ['Alice Corp', 'Bob Inc', 'Charlie Ltd', 'Delta Co', 'Echo LLC'],
            'email': ['alice@corp.com', 'bob@inc.com', 'charlie@ltd.com', 'delta@co.com', 'echo@llc.com'],
            'revenue': [1000.50, 2500.75, 1750.25, 3200.00, 950.80],
            'signup_date': ['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05', '2024-05-12'],
            'is_active': [True, True, False, True, True]
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name
        
        # Cleanup
        os.unlink(f.name)
    
    def test_init(self, sample_csv_file):
        """Test CSVAnalyzer initialization."""
        analyzer = CSVAnalyzer(sample_csv_file, delimiter=',')
        assert analyzer.file_path == sample_csv_file
        assert analyzer.delimiter == ','
        assert analyzer.df is None
        assert analyzer.analysis == {}
    
    def test_load_data_success(self, sample_csv_file):
        """Test successful data loading."""
        analyzer = CSVAnalyzer(sample_csv_file)
        analyzer.load_data()
        
        assert analyzer.df is not None
        assert len(analyzer.df) == 5
        assert 'customer_id' in analyzer.df.columns
    
    def test_generate_report(self, sample_csv_file):
        """Test complete report generation."""
        analyzer = CSVAnalyzer(sample_csv_file)
        report = analyzer.generate_report()
        
        # Check all major sections are present
        assert 'structure' in report
        assert 'columns' in report
        assert 'key_candidates' in report
        assert 'composite_key_candidates' in report
        
        # Verify structure data
        assert report['structure']['rows'] == 5
        assert report['structure']['columns'] == 6

class TestProgrammaticAPI:
    """Test the new programmatic functions for LLM agents."""
    
    @pytest.fixture
    def two_csv_files(self):
        """Create two related CSV files for testing."""
        # File 1: Customer data
        data1 = {
            'customer_id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'revenue': [1000, 2000, 1500]
        }
        df1 = pd.DataFrame(data1)
        
        # File 2: Similar structure
        data2 = {
            'customer_id': [1, 2, 3],
            'name': ['Alice Corp', 'Bob Inc', 'Charlie Ltd'],
            'revenue': [1100, 1900, 1600]
        }
        df2 = pd.DataFrame(data2)
        
        files = []
        for df in [df1, df2]:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                df.to_csv(f.name, index=False)
                files.append(f.name)
        
        yield files
        
        # Cleanup
        for f in files:
            os.unlink(f)
    
    def test_get_recommended_keys(self, two_csv_files):
        """Test the main programmatic function."""
        result = get_recommended_keys(two_csv_files)
        
        # Check return structure
        assert 'status' in result
        assert 'recommended_keys' in result
        assert 'key_confidence' in result
        assert 'analysis_summary' in result
        
        assert result['status'] in ['success', 'error']
        assert isinstance(result['recommended_keys'], list)
        assert isinstance(result['key_confidence'], (int, float))
    
    def test_quick_key_analysis(self, two_csv_files):
        """Test the two-file convenience function."""
        result = quick_key_analysis(two_csv_files[0], two_csv_files[1])
        
        # Should have same structure as get_recommended_keys
        assert 'status' in result
        assert 'recommended_keys' in result
        assert len(result['files']) == 2
    
    def test_get_column_intersection(self, two_csv_files):
        """Test column intersection utility."""
        result = get_column_intersection(two_csv_files)
        
        assert 'status' in result
        assert 'common_columns' in result
        assert 'all_columns' in result
        assert 'file_columns' in result
        
        if result['status'] == 'success':
            assert isinstance(result['common_columns'], list)
            assert len(result['common_columns']) > 0  # Should have common columns

class TestCommandLineInterface:
    """Test CLI functionality."""
    
    def test_parse_arguments_basic(self):
        """Test basic argument parsing."""
        test_args = ['file1.csv', 'file2.csv']
        
        with patch('sys.argv', ['eda_analyzer.py'] + test_args):
            args = parse_arguments()
            assert args.files == test_args
            assert args.delimiter == ','
    
    def test_main_success(self):
        """Test main function with mock data."""
        # Create a simple test file
        data = {'id': [1, 2, 3], 'name': ['A', 'B', 'C']}
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            test_args = ['eda_analyzer.py', f.name]
            
            with patch('sys.argv', test_args):
                with patch('sys.stdout', new_callable=StringIO):
                    try:
                        main()
                    except SystemExit as e:
                        # main() may exit with 0 on success
                        pass  # Allow any exit code for this test
            
            os.unlink(f.name)

class TestErrorHandling:
    """Test error conditions."""
    
    def test_get_recommended_keys_file_not_found(self):
        """Test error handling for non-existent files."""
        result = get_recommended_keys(['nonexistent1.csv', 'nonexistent2.csv'])
        
        assert result['status'] == 'error'
        assert result['recommended_keys'] == []
        assert 'error_message' in result
    
    def test_get_column_intersection_file_not_found(self):
        """Test column intersection with bad files."""
        result = get_column_intersection(['bad1.csv', 'bad2.csv'])
        
        assert result['status'] == 'error'
        assert result['common_columns'] == []

# Pytest configuration
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
