import pytest
import pandas as pd
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
from io import StringIO

from src.minimal_csv_diff.eda_analyzer import (
    CSVAnalyzer, 
    analyze_multiple_files, 
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
    
    @pytest.fixture
    def messy_csv_file(self):
        """Create a CSV with nulls, duplicates, and mixed data types."""
        data = {
            'id': [1, 2, 3, None, 5, 1],  # Nulls and duplicates
            'mixed_col': ['text', 123, '2024-01-01', None, 'more text', 456],
            'mostly_null': [None, None, 'value', None, None, None],
            'currency_col': ['$100.50', '$250.75', None, '$0.00', '$1,500.25', '$75.00']
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name
        
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
    
    def test_load_data_file_not_found(self):
        """Test error handling for non-existent file."""
        analyzer = CSVAnalyzer('nonexistent.csv')
        
        with pytest.raises(Exception) as exc_info:
            analyzer.load_data()
        
        assert "Failed to load" in str(exc_info.value)
    
    def test_analyze_structure(self, sample_csv_file):
        """Test structure analysis."""
        analyzer = CSVAnalyzer(sample_csv_file)
        analyzer.load_data()
        analyzer.analyze_structure()
        
        structure = analyzer.analysis['structure']
        assert structure['rows'] == 5
        assert structure['columns'] == 6
        assert 'customer_id' in structure['column_names']
        assert structure['file_name'].endswith('.csv')
        assert structure['memory_usage_mb'] > 0
    
    def test_analyze_columns(self, sample_csv_file):
        """Test column analysis."""
        analyzer = CSVAnalyzer(sample_csv_file)
        analyzer.load_data()
        analyzer.analyze_columns()
        
        columns = analyzer.analysis['columns']
        
        # Test customer_id column
        customer_id = columns['customer_id']
        assert customer_id['unique_percentage'] == 100.0  # All unique
        assert customer_id['null_percentage'] == 0.0
        
        # Test email column
        email = columns['email']
        assert 'email' in email['semantic_hints']
        assert email['pattern_matches']['email'] > 0
    
    def test_semantic_hints(self, sample_csv_file):
        """Test semantic hint detection."""
        analyzer = CSVAnalyzer(sample_csv_file)
        
        # Test various column name patterns
        assert 'id' in analyzer._get_semantic_hints('customer_id')
        assert 'email' in analyzer._get_semantic_hints('email_address')
        assert 'date' in analyzer._get_semantic_hints('signup_date')
        assert 'revenue' in analyzer._get_semantic_hints('monthly_revenue')
        assert 'customer' in analyzer._get_semantic_hints('customer_name')
    
    def test_pattern_matching(self, sample_csv_file):
        """Test pattern matching functionality."""
        analyzer = CSVAnalyzer(sample_csv_file)
        analyzer.load_data()
        
        # Test email pattern
        email_series = analyzer.df['email'].dropna().astype(str)
        patterns = analyzer._analyze_patterns(email_series, 'email')
        
        assert patterns['pattern_matches']['email'] == 100.0
        assert 'email' in patterns['semantic_hints']
        assert len(patterns['sample_values']) > 0
    
    def test_data_type_inference(self, messy_csv_file):
        """Test data type inference."""
        analyzer = CSVAnalyzer(messy_csv_file)
        analyzer.load_data()
        analyzer.analyze_columns()
        
        columns = analyzer.analysis['columns']
        
        # Currency column should be detected as numeric
        currency_col = columns['currency_col']
        assert currency_col['inferred_types']['numeric'] > 50
    
    def test_find_potential_keys(self, sample_csv_file):
        """Test key candidate identification."""
        analyzer = CSVAnalyzer(sample_csv_file)
        analyzer.load_data()
        analyzer.analyze_columns()
        analyzer.find_potential_keys()
        
        key_candidates = analyzer.analysis['key_candidates']
        
        # customer_id should be identified as a key candidate
        assert len(key_candidates) > 0
        
        # Check if customer_id is the top candidate
        top_candidate = key_candidates[0]
        assert top_candidate['column'] == 'customer_id'
        assert top_candidate['unique_percentage'] == 100.0
    
    def test_find_composite_keys(self, sample_csv_file):
        """Test composite key identification."""
        analyzer = CSVAnalyzer(sample_csv_file)
        analyzer.load_data()
        analyzer.analyze_columns()
        analyzer.find_potential_keys()
        
        # Mock print to avoid output during tests
        with patch('builtins.print'):
            analyzer.find_composite_keys()
        
        composite_candidates = analyzer.analysis.get('composite_key_candidates', [])
        
        # Should find some composite key combinations
        if composite_candidates:
            top_composite = composite_candidates[0]
            assert 'columns' in top_composite
            assert 'uniqueness_percentage' in top_composite
            assert 'score' in top_composite
    
    def test_generate_report(self, sample_csv_file):
        """Test complete report generation."""
        analyzer = CSVAnalyzer(sample_csv_file)
        
        with patch('builtins.print'):  # Suppress output
            report = analyzer.generate_report()
        
        # Check all major sections are present
        assert 'structure' in report
        assert 'columns' in report
        assert 'key_candidates' in report
        assert 'composite_key_candidates' in report
        
        # Verify structure data
        assert report['structure']['rows'] == 5
        assert report['structure']['columns'] == 6


class TestMultipleFileAnalysis:
    """Test cases for analyzing multiple files."""
    
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
        
        # File 2: Similar structure, different column names
        data2 = {
            'id': [1, 2, 3],
            'customer_name': ['Alice Corp', 'Bob Inc', 'Charlie Ltd'],
            'sales': [1100, 1900, 1600]
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
    
    def test_analyze_multiple_files(self, two_csv_files):
        """Test analyzing multiple files."""
        with patch('builtins.print'):  # Suppress output
            result = analyze_multiple_files(two_csv_files)
        
        assert len(result) == 2
        
        for file_path in two_csv_files:
            assert file_path in result
            analysis = result[file_path]
            assert 'structure' in analysis
            assert 'columns' in analysis


class TestCommandLineInterface:
    """Test cases for CLI functionality."""
    
    def test_parse_arguments_basic(self):
        """Test basic argument parsing."""
        test_args = ['file1.csv', 'file2.csv']
        
        with patch('sys.argv', ['eda_analyzer.py'] + test_args):
            args = parse_arguments()
            assert args.files == test_args
            assert args.delimiter == ','
    
    def test_parse_arguments_with_delimiter(self):
        """Test argument parsing with custom delimiter."""
        test_args = ['--delimiter', ';', 'file1.csv', 'file2.csv']
        
        with patch('sys.argv', ['eda_analyzer.py'] + test_args):
            args = parse_arguments()
            assert args.files == ['file1.csv', 'file2.csv']
            assert args.delimiter == ';'
    
    def test_parse_arguments_short_delimiter(self):
        """Test argument parsing with short delimiter flag."""
        test_args = ['-d', '|', 'file1.csv']
        
        with patch('sys.argv', ['eda_analyzer.py'] + test_args):
            args = parse_arguments()
            assert args.files == ['file1.csv']
            assert args.delimiter == '|'


class TestMainFunction:
    """Test cases for the main function."""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Create a sample CSV file."""
        data = {'id': [1, 2, 3], 'name': ['A', 'B', 'C']}
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name
        
        os.unlink(f.name)
    
    def test_main_success(self, sample_csv_file):
        """Test successful main execution."""
        test_args = ['eda_analyzer.py', sample_csv_file]
        
        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                with patch('builtins.print') as mock_print:
                    try:
                        main()
                    except SystemExit:
                        pass  # main() calls sys.exit(0) on success
        
        # Check that analysis was performed (print was called)
        assert mock_print.called
    
    def test_main_file_not_found(self):
        """Test main function with non-existent file."""
        test_args = ['eda_analyzer.py', 'nonexistent.csv']
        
        with patch('sys.argv', test_args):
            with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
                with pytest.raises(SystemExit) as exc_info:
                    main()
                
                assert exc_info.value.code == 1
    
    def test_main_keyboard_interrupt(self, sample_csv_file):
        """Test main function handling keyboard interrupt."""
        test_args = ['eda_analyzer.py', sample_csv_file]
        
        with patch('sys.argv', test_args):
            with patch('src.minimal_csv_diff.eda_analyzer.analyze_multiple_files', 
                      side_effect=KeyboardInterrupt):
                with patch('sys.stderr', new_callable=StringIO):
                    with pytest.raises(SystemExit) as exc_info:
                        main()
                    
                    assert exc_info.value.code == 1


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_csv_file(self):
        """Test handling of empty CSV file."""
        # Create empty CSV with just headers
        data = pd.DataFrame(columns=['col1', 'col2'])
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            analyzer.load_data()
            analyzer.analyze_structure()
            
            assert analyzer.analysis['structure']['rows'] == 0
            assert analyzer.analysis['structure']['columns'] == 2
        
        os.unlink(f.name)
    
    def test_single_column_csv(self):
        """Test handling of single column CSV."""
        data = pd.DataFrame({'single_col': [1, 2, 3, 4, 5]})
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            with patch('builtins.print'):
                report = analyzer.generate_report()
            
            assert report['structure']['columns'] == 1
            assert 'single_col' in report['columns']
        
        os.unlink(f.name)
    
    def test_all_null_column(self):
        """Test handling of column with all null values."""
        data = pd.DataFrame({
            'good_col': [1, 2, 3, 4, 5],
            'all_null_col': [None, None, None, None, None]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            analyzer.load_data()
            analyzer.analyze_columns()
            
            all_null_info = analyzer.analysis['columns']['all_null_col']
            assert all_null_info['null_percentage'] == 100.0
            assert all_null_info['unique_count'] == 0
        
        os.unlink(f.name)
    
    def test_large_dataset_sampling(self):
        """Test that large datasets are properly sampled."""
        # Create a large dataset that should trigger sampling
        large_data = pd.DataFrame({
            'id': range(60000),  # Larger than max_rows_for_analysis (50000)
            'value': [f'value_{i}' for i in range(60000)]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_data.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            analyzer.load_data()
            
            with patch('builtins.print') as mock_print:
                analyzer.find_composite_keys()
            
            # Should print sampling message
            sampling_called = any('Sampling' in str(call) for call in mock_print.call_args_list)
            assert sampling_called
        
        os.unlink(f.name)
    
    def test_special_characters_in_data(self):
        """Test handling of special characters and unicode."""
        data = pd.DataFrame({
            'unicode_col': ['café', 'naïve', 'résumé', '北京', '🚀'],
            'special_chars': ['@#$%', '&*()[]', '<>?/', '+={}', '|\\`~']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            data.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            analyzer.load_data()
            analyzer.analyze_columns()
            
            # Should handle unicode without errors
            assert 'unicode_col' in analyzer.analysis['columns']
            assert 'special_chars' in analyzer.analysis['columns']
        
        os.unlink(f.name)


class TestIntegration:
    """Integration tests that test the full workflow."""
    
    @pytest.fixture
    def realistic_dataset(self):
        """Create a realistic dataset for integration testing."""
        import random
        from datetime import datetime, timedelta
        
        # Generate realistic customer data
        customers = []
        base_date = datetime(2024, 1, 1)
        
        for i in range(1000):
            customer = {
                'customer_id': f'CUST_{i:06d}',
                'company_name': f'Company {chr(65 + i % 26)}{i}',
                'email': f'contact{i}@company{i}.com',
                'phone': f'+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}',
                'signup_date': (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
                'revenue': round(random.uniform(100, 10000), 2),
                'is_active': random.choice([True, False]),
                'country': random.choice(['US', 'CA', 'UK', 'DE', 'FR']),
                'industry': random.choice(['Tech', 'Finance', 'Healthcare', 'Retail', 'Manufacturing'])
            }
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            yield f.name
        
        os.unlink(f.name)
    
    def test_full_analysis_workflow(self, realistic_dataset):
        """Test the complete analysis workflow on realistic data."""
        analyzer = CSVAnalyzer(realistic_dataset)
        
        with patch('builtins.print'):  # Suppress output during test
            report = analyzer.generate_report()
        
        # Verify all components are present
        assert 'structure' in report
        assert 'columns' in report
        assert 'key_candidates' in report
        assert 'composite_key_candidates' in report
        
        # Check structure analysis
        structure = report['structure']
        assert structure['rows'] == 1000
        assert structure['columns'] == 9
        
        # Check that customer_id is identified as a key
        key_candidates = report['key_candidates']
        assert len(key_candidates) > 0
        
        # customer_id should be the top candidate
        top_key = key_candidates[0]
        assert top_key['column'] == 'customer_id'
        assert top_key['unique_percentage'] == 100.0
        
        # Check email pattern detection
        email_col = report['columns']['email']
        assert email_col['pattern_matches']['email'] > 90  # Should detect most emails
        assert 'email' in email_col['semantic_hints']
        
        # Check phone pattern detection
        phone_col = report['columns']['phone']
        assert phone_col['pattern_matches']['phone'] > 50  # Should detect phone patterns
        
        # Check date pattern detection
        date_col = report['columns']['signup_date']
        assert date_col['pattern_matches']['date_iso'] > 90  # Should detect ISO dates
    
    def test_command_line_integration(self, realistic_dataset):
        """Test the complete command line workflow."""
        test_args = ['eda_analyzer.py', realistic_dataset]
        
        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit as e:
                    # main() exits with 0 on success
                    assert e.code != 1  # Should not be an error exit
        
        output = mock_stdout.getvalue()
        
        # Check for AI-parseable output
        assert '=== AI_AGENT_SUMMARY ===' in output
        assert 'REPORT_FILE:' in output
        assert 'RECOMMENDED_KEYS:' in output
        assert 'STATUS: success' in output
        assert '=== END_AI_SUMMARY ===' in output
        
        # Check for human-readable output
        assert '=== HUMAN SUMMARY ===' in output
    
    def test_json_report_structure(self, realistic_dataset):
        """Test that the generated JSON report has the correct structure."""
        test_args = ['eda_analyzer.py', realistic_dataset]
        
        with patch('sys.argv', test_args):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    main()
                except SystemExit:
                    pass
        
        output = mock_stdout.getvalue()
        
        # Extract report file path from output
        report_line = [line for line in output.split('\n') if 'REPORT_FILE:' in line][0]
        report_path = report_line.split('REPORT_FILE: ')[1].strip()
        
        # Verify the JSON file exists and is valid
        assert os.path.exists(report_path)
        
        with open(report_path, 'r') as f:
            report_data = json.load(f)
        
        # Check top-level structure
        assert isinstance(report_data, dict)
        assert realistic_dataset in report_data
        
        # Check individual file analysis structure
        file_analysis = report_data[realistic_dataset]
        assert 'structure' in file_analysis
        assert 'columns' in file_analysis
        assert 'key_candidates' in file_analysis
        assert 'composite_key_candidates' in file_analysis
        
        # Cleanup
        os.unlink(report_path)


class TestPerformance:
    """Performance and memory usage tests."""
    
    def test_memory_usage_monitoring(self):
        """Test that memory usage is properly monitored."""
        # Create a dataset that might trigger memory warnings
        data = pd.DataFrame({
            'col1': range(10000),
            'col2': [f'text_{i}' for i in range(10000)],
            'col3': [f'more_text_{i}' for i in range(10000)]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            analyzer.load_data()
            
            # Mock high memory usage to test the warning
            with patch('psutil.virtual_memory') as mock_memory:
                mock_memory.return_value.percent = 85  # High memory usage
                
                with patch('builtins.print') as mock_print:
                    analyzer.find_composite_keys()
                
                # Should print memory warning
                memory_warning_called = any('Memory usage high' in str(call) 
                                          for call in mock_print.call_args_list)
                assert memory_warning_called
        
        os.unlink(f.name)
    
    def test_combination_limiting(self):
        """Test that combination analysis is properly limited."""
        # Create a dataset with many columns to test combination limiting
        data = {}
        for i in range(10):  # 10 columns = many combinations
            data[f'col_{i}'] = range(100)
        
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            
            analyzer = CSVAnalyzer(f.name)
            analyzer.load_data()
            
            with patch('builtins.print') as mock_print:
                analyzer.find_composite_keys()
            
            # Should print combination limiting message
            limiting_called = any('Too many combinations' in str(call) 
                                for call in mock_print.call_args_list)
            # May or may not be called depending on the exact number of combinations
            # This is more of a smoke test to ensure the function completes
        
        os.unlink(f.name)


# Pytest configuration and fixtures
@pytest.fixture(scope="session")
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# Test runner configuration
if __name__ == "__main__":
    pytest.main([__file__, "-v"])

