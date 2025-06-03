"""
CSV EDA Analyzer - Exploratory Data Analysis for CSV files

This module provides both programmatic API for LLM agents and CLI interface for humans.
The programmatic API is designed for automated CSV processing workflows.

Programmatic Usage (LLM Agents):
    from minimal_csv_diff.eda_analyzer import get_recommended_keys, CSVAnalyzer
    
    # Quick two-file analysis
    result = get_recommended_keys(['file1.csv', 'file2.csv'])
    keys = result['recommended_keys']
    
    # Detailed single file analysis
    analyzer = CSVAnalyzer('data.csv')
    report = analyzer.generate_report()

CLI Usage (Humans):
    python -m minimal_csv_diff.eda_analyzer file1.csv file2.csv
    csv-analyze file1.csv file2.csv
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import re
from collections import Counter
from itertools import combinations
import psutil
import tempfile
import uuid
import os
import argparse
import sys

class CSVAnalyzer:
    """
    Analyzes CSV files to extract structural and statistical information.
    
    This class provides detailed analysis of individual CSV files including:
    - Column data types and patterns
    - Potential key columns (single and composite)
    - Data quality metrics
    - Sample values and statistics
    """
    
    def __init__(self, file_path: str, delimiter: str = ','):
        """
        Initialize CSV analyzer for a single file.
        
        Args:
            file_path (str): Path to the CSV file
            delimiter (str): CSV delimiter character (default: ',')
        """
        self.file_path = file_path
        self.delimiter = delimiter
        self.df = None
        self.analysis = {}
        
    def load_data(self):
        """Load CSV with robust error handling and encoding detection."""
        try:
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    self.df = pd.read_csv(
                        self.file_path, 
                        delimiter=self.delimiter,
                        encoding=encoding,
                        low_memory=False
                    )
                    break
                except UnicodeDecodeError:
                    continue
        except Exception as e:
            raise Exception(f"Failed to load {self.file_path}: {str(e)}")
    
    def analyze_structure(self):
        """Analyze basic file structure and metadata."""
        self.analysis['structure'] = {
            'file_name': Path(self.file_path).name,
            'rows': len(self.df),
            'columns': len(self.df.columns),
            'column_names': list(self.df.columns),
            'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024**2
        }
    
    def analyze_columns(self):
        """Perform deep analysis of each column."""
        column_analysis = {}
        
        for col in self.df.columns:
            series = self.df[col]
            
            # Basic stats
            col_info = {
                'dtype': str(series.dtype),
                'null_count': series.isnull().sum(),
                'null_percentage': (series.isnull().sum() / len(series)) * 100,
                'unique_count': series.nunique(),
                'unique_percentage': (series.nunique() / len(series)) * 100
            }
            
            # Pattern analysis for potential matching
            non_null_series = series.dropna().astype(str)
            if len(non_null_series) > 0:
                col_info.update(self._analyze_patterns(non_null_series, col))
                col_info.update(self._analyze_data_types(non_null_series))
            
            column_analysis[col] = col_info
            
        self.analysis['columns'] = column_analysis
    
    def _analyze_patterns(self, series: pd.Series, col_name: str) -> Dict:
        """Analyze string patterns in column values for type detection."""
        patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'[\+]?[1-9]?[\d\s\-\(\)]{7,15}',
            'date_iso': r'\d{4}-\d{2}-\d{2}',
            'date_us': r'\d{1,2}/\d{1,2}/\d{4}',
            'id_numeric': r'^\d+$',
            'id_alphanumeric': r'^[A-Za-z0-9]+$',
            'currency': r'[\$£€¥]?\d+\.?\d*',
            'percentage': r'\d+\.?\d*%',
            'postal_code': r'^\d{5}(-\d{4})?$|^[A-Z]\d[A-Z]\s?\d[A-Z]\d$'
        }
        
        pattern_matches = {}
        sample_values = series.head(100).tolist()
        
        for pattern_name, pattern in patterns.items():
            matches = sum(1 for val in sample_values if re.match(pattern, str(val)))
            pattern_matches[pattern_name] = (matches / len(sample_values)) * 100
        
        semantic_hints = self._get_semantic_hints(col_name.lower())
        
        return {
            'pattern_matches': pattern_matches,
            'semantic_hints': semantic_hints,
            'sample_values': sample_values[:10],
            'value_lengths': {
                'min': series.str.len().min(),
                'max': series.str.len().max(),
                'avg': series.str.len().mean()
            }
        }
    
    def _get_semantic_hints(self, col_name: str) -> List[str]:
        """Extract semantic meaning from column names using keyword matching."""
        hints = []
        
        semantic_patterns = {
            'id': ['id', 'key', 'identifier', 'ref'],
            'name': ['name', 'title', 'label'],
            'email': ['email', 'mail'],
            'phone': ['phone', 'tel', 'mobile'],
            'address': ['address', 'addr', 'street', 'city', 'state', 'zip'],
            'date': ['date', 'time', 'created', 'updated', 'modified', 'month', 'year', 'calendar'],
            'amount': ['amount', 'price', 'cost', 'value', 'total'],
            'status': ['status', 'state', 'flag', 'active'],
            'revenue': ['revenue', 'income', 'sales', 'earnings'],
            'difference': ['diff', 'difference', 'delta', 'change'],
            'financial': ['amount', 'price', 'cost', 'value', 'total', 'revenue'],
            'customer': ['customer', 'client', 'user', 'account', 'company']
        }
        
        for category, keywords in semantic_patterns.items():
            if any(keyword in col_name for keyword in keywords):
                hints.append(category)
        
        return hints
    
    def _analyze_data_types(self, series: pd.Series) -> Dict:
        """Infer actual data types beyond pandas dtype using value analysis."""
        sample = series.head(1000)
        
        type_counts = {
            'numeric': 0,
            'date': 0,
            'boolean': 0,
            'categorical': 0,
            'text': 0
        }
        
        for val in sample:
            val_str = str(val).strip()
            
            # Numeric check
            try:
                float(val_str.replace(',', '').replace('$', '').replace('%', ''))
                type_counts['numeric'] += 1
                continue
            except ValueError:
                pass
            
            # Date check
            try:
                pd.to_datetime(val_str)
                type_counts['date'] += 1
                continue
            except:
                pass
            
            # Boolean check
            if val_str.lower() in ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n']:
                type_counts['boolean'] += 1
                continue
            
            # Categorical (short repeated values)
            if len(val_str) < 50 and series.nunique() < len(series) * 0.5:
                type_counts['categorical'] += 1
            else:
                type_counts['text'] += 1
        
        # Convert to percentages
        total = sum(type_counts.values())
        if total > 0:
            type_percentages = {k: (v/total)*100 for k, v in type_counts.items()}
        else:
            type_percentages = type_counts
            
        return {'inferred_types': type_percentages}
    
    def find_potential_keys(self):
        """Identify potential single-column key candidates."""
        key_candidates = []
        
        for col, info in self.analysis['columns'].items():
            score = 0
            reasons = []
            
            # High uniqueness
            if info['unique_percentage'] > 70:
                score += 2
                reasons.append('high_uniqueness')
            elif info['unique_percentage'] > 50:
                score += 1
                reasons.append('moderate_uniqueness')
            
            # Semantic hints
            if 'id' in info.get('semantic_hints', []):
                score += 2
                reasons.append('semantic_id')
            
            # Pattern matching
            patterns = info.get('pattern_matches', {})
            if patterns.get('id_numeric', 0) > 80 or patterns.get('id_alphanumeric', 0) > 80:
                score += 2
                reasons.append('id_pattern')
            
            # Low null percentage
            if info['null_percentage'] < 5:
                score += 1
                reasons.append('low_nulls')
            
            if score >= 2:
                key_candidates.append({
                    'column': col,
                    'score': score,
                    'reasons': reasons,
                    'unique_percentage': info['unique_percentage'],
                    'null_percentage': info['null_percentage']
                })
        
        # Sort by score
        key_candidates.sort(key=lambda x: x['score'], reverse=True)
        self.analysis['key_candidates'] = key_candidates
    
    def find_composite_keys(self):
        """Find composite key combinations using memory-efficient analysis."""
        if self.df is None:
            return
            
        # Memory and performance constraints
        max_rows_for_analysis = 50000
        max_combinations_per_size = 100
        target_uniqueness = 95.0
        max_columns = min(6, len(self.df.columns))
        
        # Sample data if too large
        df_sample = self.df
        if len(self.df) > max_rows_for_analysis:
            df_sample = self.df.sample(n=max_rows_for_analysis, random_state=42)
        
        # Get candidate columns (exclude columns with >50% nulls)
        candidate_columns = []
        for col in self.df.columns:
            null_pct = (self.df[col].isnull().sum() / len(self.df)) * 100
            if null_pct < 50:
                candidate_columns.append(col)
        
        composite_candidates = []
        
        # Test combinations from size 2 to max_columns
        for combo_size in range(2, max_columns + 1):
            # Generate all combinations of this size
            combos = list(combinations(candidate_columns, combo_size))
            
            # Limit combinations to prevent memory issues
            if len(combos) > max_combinations_per_size:
                combos = combos[:max_combinations_per_size]
            
            best_for_this_size = None
            
            for combo_cols in combos:
                # Check available memory
                if psutil.virtual_memory().percent > 80:
                    break
                
                try:
                    # Calculate uniqueness with null handling
                    combo_data = df_sample[list(combo_cols)].dropna()
                    
                    if len(combo_data) == 0:
                        continue
                        
                    unique_combinations = len(combo_data.drop_duplicates())
                    total_valid_rows = len(combo_data)
                    uniqueness_pct = (unique_combinations / total_valid_rows) * 100
                    
                    # Calculate score
                    base_score = uniqueness_pct * 0.8
                    simplicity_penalty = (combo_size - 2) * 3
                    final_score = base_score - simplicity_penalty
                    
                    # Null impact analysis
                    null_rows = len(df_sample) - len(combo_data)
                    null_impact_pct = (null_rows / len(df_sample)) * 100
                    
                    candidate = {
                        'columns': list(combo_cols),
                        'column_count': combo_size,
                        'uniqueness_percentage': round(uniqueness_pct, 2),
                        'score': round(final_score, 2),
                        'null_impact_percentage': round(null_impact_pct, 2),
                        'total_valid_rows': total_valid_rows,
                        'unique_combinations': unique_combinations,
                        'duplicate_count': total_valid_rows - unique_combinations
                    }
                    
                    # Track best for this size
                    if best_for_this_size is None or final_score > best_for_this_size['score']:
                        best_for_this_size = candidate
                    
                    # Early termination if we found excellent uniqueness
                    if uniqueness_pct >= target_uniqueness:
                        composite_candidates.append(candidate)
                        break
                        
                except Exception as e:
                    continue
            
            # Add best candidate from this size if no perfect match found
            if best_for_this_size and best_for_this_size not in composite_candidates:
                composite_candidates.append(best_for_this_size)
            
            # Stop if we found a great key and want to save time
            if best_for_this_size and best_for_this_size['uniqueness_percentage'] >= target_uniqueness:
                break
        
        # Sort by score (best first)
        composite_candidates.sort(key=lambda x: x['score'], reverse=True)
        
        # Keep top 5 candidates
        self.analysis['composite_key_candidates'] = composite_candidates[:5]
    
    def generate_report(self) -> Dict:
        """
        Generate complete analysis report.
        
        Returns:
            Dict: Complete analysis including structure, columns, and key candidates
        """
        self.load_data()
        self.analyze_structure()
        self.analyze_columns()
        self.find_potential_keys()
        self.find_composite_keys()
        
        return self.analysis

def analyze_multiple_files(file_paths: List[str], delimiter: str = ',') -> Dict:
    """
    Analyze multiple CSV files individually.
    
    Args:
        file_paths (List[str]): List of CSV file paths to analyze
        delimiter (str): CSV delimiter character
        
    Returns:
        Dict: Analysis results for each file, keyed by file path
    """
    analyses = {}
    
    for file_path in file_paths:
        analyzer = CSVAnalyzer(file_path, delimiter)
        analyses[file_path] = analyzer.generate_report()
    
    return analyses

def get_recommended_keys(file_paths: List[str], delimiter: str = ',') -> Dict[str, Any]:
    """
    Get recommended key columns for CSV comparison without file I/O overhead.
    
    Designed specifically for LLM agents and programmatic usage.
    
    Args:
        file_paths (List[str]): List of CSV file paths to analyze
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        Dict containing:
            - recommended_keys (List[str]): Best key columns for comparison
            - key_type (str): 'single', 'composite', or 'manual_required'  
            - key_confidence (float): Confidence score 0-100
            - target_file (str): File to use as comparison baseline
            - analysis_summary (Dict): Key metrics for each file
            - status (str): 'success' or 'error'
            - error_message (str): Error details if status is 'error'
    
    Example:
        >>> result = get_recommended_keys(['data1.csv', 'data2.csv'])
        >>> if result['status'] == 'success':
        ...     keys = result['recommended_keys']
        ...     confidence = result['key_confidence']
        >>> # Use keys with main.py diff_csv function
    
    LLM Agent Workflow:
        1. Call this function with CSV file paths
        2. Check result['status'] for success/error
        3. Use result['recommended_keys'] with diff_csv()
        4. Access result['analysis_summary'] for additional insights
    """
    try:
        # Analyze files
        analysis_result = analyze_multiple_files(file_paths, delimiter)
        
        # Extract recommendations (same logic as main())
        recommended_keys = []
        key_confidence = 0
        key_type = "none"
        target_file = file_paths[0] if file_paths else None
        
        if target_file and target_file in analysis_result:
            analysis = analysis_result[target_file]
            
            # Try composite keys first
            composite_keys = analysis.get('composite_key_candidates', [])
            if composite_keys and composite_keys[0]['uniqueness_percentage'] > 90:
                recommended_keys = composite_keys[0]['columns']
                key_confidence = composite_keys[0]['uniqueness_percentage']
                key_type = "composite"
            else:
                # Fallback to single column keys
                single_keys = analysis.get('key_candidates', [])
                if single_keys and single_keys[0]['unique_percentage'] > 70:
                    recommended_keys = [single_keys[0]['column']]
                    key_confidence = single_keys[0]['unique_percentage']
                    key_type = "single"
        
        if not recommended_keys:
            key_type = "manual_required"
        
        # Build summary for LLM agent
        analysis_summary = {}
        for file_path, analysis in analysis_result.items():
            analysis_summary[file_path] = {
                'rows': analysis['structure']['rows'],
                'columns': analysis['structure']['columns'],
                'column_names': analysis['structure']['column_names'],
                'best_single_key': analysis.get('key_candidates', [{}])[0].get('column') if analysis.get('key_candidates') else None,
                'best_composite_key': analysis.get('composite_key_candidates', [{}])[0].get('columns') if analysis.get('composite_key_candidates') else None
            }
        
        return {
            'recommended_keys': recommended_keys,
            'key_type': key_type,
            'key_confidence': key_confidence,
            'target_file': target_file,
            'analysis_summary': analysis_summary,
            'files': file_paths,
            'status': 'success',
            'error_message': None
        }
        
    except Exception as e:
        return {
            'recommended_keys': [],
            'key_type': 'error',
            'key_confidence': 0,
            'target_file': None,
            'analysis_summary': {},
            'files': file_paths,
            'status': 'error',
            'error_message': str(e)
        }

def quick_key_analysis(file1: str, file2: str, delimiter: str = ',') -> Dict[str, Any]:
    """
    Fast key recommendation for two-file comparison (most common LLM agent use case).
    
    Optimized version of get_recommended_keys() for the common scenario of
    comparing exactly two CSV files.
    
    Args:
        file1 (str): First CSV file path
        file2 (str): Second CSV file path  
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        Dict with same structure as get_recommended_keys() but optimized for two files
    
    LLM Agent Usage:
        >>> keys_info = quick_key_analysis('old_data.csv', 'new_data.csv')
        >>> if keys_info['status'] == 'success':
        ...     from minimal_csv_diff.main import diff_csv
        ...     diff_csv(file1, file2, delimiter, keys_info['recommended_keys'])
    """
    return get_recommended_keys([file1, file2], delimiter)

def get_column_intersection(file_paths: List[str], delimiter: str = ',') -> Dict[str, Any]:
    """
    Get common columns across multiple CSV files for key selection.
    
    Useful for LLM agents to understand what columns are available for comparison
    before running full analysis.
    
    Args:
        file_paths (List[str]): List of CSV file paths
        delimiter (str): CSV delimiter character (default: ',')
    
    Returns:
        Dict containing:
            - common_columns (List[str]): Columns present in all files
            - all_columns (List[str]): All unique columns across files
            - file_columns (Dict): Columns for each file
            - status (str): 'success' or 'error'
            - error_message (str): Error details if status is 'error'
    
    LLM Agent Usage:
        >>> cols = get_column_intersection(['file1.csv', 'file2.csv'])
        >>> if cols['status'] == 'success':
        ...     available_keys = cols['common_columns']
    """
    try:
        file_columns = {}
        all_columns = set()
        
        for file_path in file_paths:
            df = pd.read_csv(file_path, delimiter=delimiter, nrows=0)  # Just get headers
            columns = list(df.columns)
            file_columns[file_path] = columns
            all_columns.update(columns)
        
        # Find intersection
        if file_columns:
            common_columns = set(list(file_columns.values())[0])
            for columns in file_columns.values():
                common_columns = common_columns.intersection(set(columns))
            common_columns = list(common_columns)
        else:
            common_columns = []
        
        return {
            'common_columns': common_columns,
            'all_columns': list(all_columns),
            'file_columns': file_columns,
            'status': 'success',
            'error_message': None
        }
        
    except Exception as e:
        return {
            'common_columns': [],
            'all_columns': [],
            'file_columns': {},
            'status': 'error',
            'error_message': str(e)
        }

# CLI INTERFACE FOR HUMANS

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="CSV EDA Analyzer - Analyze CSV files for data comparison",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  csv-analyze data1.csv data2.csv
  csv-analyze --delimiter ';' file1.csv file2.csv file3.csv
  python -m minimal_csv_diff.eda_analyzer data/*.csv

LLM Agent Usage:
  1. Import functions: from minimal_csv_diff.eda_analyzer import get_recommended_keys
  2. Get recommendations: result = get_recommended_keys(['file1.csv', 'file2.csv'])
  3. Use with diff tool: diff_csv(file1, file2, delimiter, result['recommended_keys'])
        """
    )
    
    parser.add_argument(
        'files',
        nargs='+',
        help='CSV files to analyze (at least one required)'
    )
    
    parser.add_argument(
        '--delimiter', '-d',
        default=',',
        help='CSV delimiter character (default: ",")'
    )
    
    return parser.parse_args()

def main():
    """
    Main entry point for the EDA analyzer CLI.
    
    This function is designed for human users. LLM agents should use the
    programmatic functions like get_recommended_keys() instead.
    
    Processes command line arguments, analyzes files, and outputs results
    in both human-readable and LLM-parseable formats.
    """
    try:
        args = parse_arguments()
        
        # Validate files exist
        for file_path in args.files:
            if not os.path.exists(file_path):
                print(f"Error: File not found: {file_path}", file=sys.stderr)
                sys.exit(1)
        
        print("Starting EDA analysis...")
        analysis_result = analyze_multiple_files(args.files, delimiter=args.delimiter)
        
        # Generate unique filename with UUID
        session_id = str(uuid.uuid4())
        temp_dir = tempfile.gettempdir()
        report_filename = f"eda_report_{session_id}.json"
        report_path = os.path.join(temp_dir, report_filename)
        
        # Save LLM-readable report to temp directory
        with open(report_path, 'w') as f:
            json.dump(analysis_result, f, indent=2, default=str)
        
        print(f"EDA report generated: {report_path}")
        
        # LLM-PARSEABLE OUTPUT
        print("\n=== AI_AGENT_SUMMARY ===")
        print(f"REPORT_FILE: {report_path}")
        print(f"SESSION_ID: {session_id}")
        
        # Extract best key recommendations from first file (use as target)
        recommended_keys = []
        key_confidence = 0
        key_type = "none"
        target_file = args.files[0] if args.files else None
        
        if target_file and target_file in analysis_result:
            analysis = analysis_result[target_file]
            
            # Try composite keys first
            composite_keys = analysis.get('composite_key_candidates', [])
            if composite_keys and composite_keys[0]['uniqueness_percentage'] > 90:
                recommended_keys = composite_keys[0]['columns']
                key_confidence = composite_keys[0]['uniqueness_percentage']
                key_type = "composite"
            else:
                # Fallback to single column keys
                single_keys = analysis.get('key_candidates', [])
                if single_keys and single_keys[0]['unique_percentage'] > 70:
                    recommended_keys = [single_keys[0]['column']]
                    key_confidence = single_keys[0]['unique_percentage']
                    key_type = "single"
        
        if recommended_keys:
            print(f"RECOMMENDED_KEYS: {','.join(recommended_keys)}")
            print(f"KEY_TYPE: {key_type}")
            print(f"KEY_CONFIDENCE: {key_confidence:.1f}")
        else:
            print("RECOMMENDED_KEYS: NONE")
            print("KEY_TYPE: manual_required")
            print("KEY_CONFIDENCE: 0")
        
        # File info
        print(f"FILES: {','.join(args.files)}")
        print(f"TARGET_FILE: {target_file}")
        print(f"STATUS: success")
        
        print("=== END_AI_SUMMARY ===")
        
        # Human-readable summary
        print("\n=== HUMAN SUMMARY ===")
        for file_path, analysis in analysis_result.items():
            print(f"\nFile: {file_path}")
            print(f"  Rows: {analysis['structure']['rows']:,}")
            print(f"  Columns: {analysis['structure']['columns']}")
            
            # Show column names
            col_names = analysis['structure']['column_names']
            if len(col_names) <= 5:
                print(f"  Column names: {', '.join(col_names)}")
            else:
                print(f"  Column names: {', '.join(col_names[:3])}, ... (+{len(col_names)-3} more)")
            
            # Single column keys
            single_keys = analysis.get('key_candidates', [])
            if single_keys:
                best_single = single_keys[0]
                print(f"  Best single key: {best_single['column']} ({best_single['unique_percentage']:.1f}% unique)")
            
            # Composite keys
            composite_keys = analysis.get('composite_key_candidates', [])
            if composite_keys:
                best_composite = composite_keys[0]
                print(f"  Best composite key: {best_composite['columns']} ({best_composite['uniqueness_percentage']:.1f}% unique)")
        
        print(f"\nDetailed analysis saved to: {report_path}")
        
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        print("STATUS: error", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
