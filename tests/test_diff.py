import os
import pandas as pd
import tempfile
from src.minimal_csv_diff.main import diff_csv

def test_diff_csv_produces_expected_output():
    # Paths to demo files
    file1 = os.path.join(os.path.dirname(__file__), '../demo/file1.csv')
    file2 = os.path.join(os.path.dirname(__file__), '../demo/file2.csv')
    expected_output = os.path.join(os.path.dirname(__file__), '../demo/diff.csv')

    # Use a temp file for output
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        output_file = tmp.name

    # Run the diff
    diff_csv(file1, file2, delimiter=',', key_columns=['id'], output_file=output_file)

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
