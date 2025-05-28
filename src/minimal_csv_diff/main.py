import os
import pandas as pd
import csv

def main():
    """Main entry point for csv-diff command."""
    workdir = os.getcwd()
    diff_workdir = input(f'Workdir is "{workdir}".\nEnter to confirm or input the full path to the directory containing the CSV files to compare: \n> ')
    if diff_workdir.strip():
        workdir = diff_workdir

    os.chdir(workdir)
    print(f'current workdir is :{workdir}')
    delimiter = input('input the file delimiter: \n> ')

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
    
    print("\n" + "-" * 50)

    csv_file1 = csv_files[file1_index]
    csv_file2 = csv_files[file2_index]

    # Load dataframes
    df_list = [pd.read_csv(f, delimiter=delimiter, converters={i: str for i in range(100)}, quoting=csv.QUOTE_MINIMAL) for f in [csv_file1, csv_file2]]
    for df in df_list:
        df.replace('', None, inplace=True)

    # Build the column pool
    column_pool = []
    for df in df_list:
        column_pool.extend(df.columns)
    column_pool = list(set(column_pool))

    # Build the unique pool
    common_pool = column_pool.copy()
    for df in df_list:
        for column in column_pool:
            if column not in df.columns:
                common_pool.remove(column)

    # Start diff
    df1 = df_list[0][[col for col in common_pool]]
    df2 = df_list[1][[col for col in common_pool]]

    # Merge the two DataFrames
    merged_df = pd.merge(df1, df2, indicator=True, how='outer')

    # Select rows present in df1 but not in df2
    unique = merged_df[(merged_df['_merge'] == 'left_only') | (merged_df['_merge'] == 'right_only')]

    # Lambda to flag diff columns with filename
    def flag_column(value):
        if value == 'left_only':
            return csv_file1
        elif value == 'right_only':
            return csv_file2
        else:
            return 'both?'

    if unique.shape[0] > 0:
        # Disable the warning
        pd.options.mode.chained_assignment = None
        print('Found differences between the two files.')
        print("\n" + "-" * 50)
        
        unique['source'] = unique['_merge'].apply(flag_column)
        unique['result'] = unique['_merge']

        # Reorder columns
        columns = ['source', 'result'] + [col for col in unique.columns if col not in ['source', 'result']]
        unique = unique[columns]

        print("\nSelect (in order) a surrogate key / PK to order the results.\nAvailable columns for concatenation:")
        print("\n" + "-" * 50)

        # Prompt user to select columns to concatenate
        for idx, col in enumerate(common_pool):
            print(f"{idx}: {col}")

        try:
            selected_indices = list(map(int, input("Enter indices of columns to concatenate (comma-separated): ").split(',')))
            selected_columns = [common_pool[i] for i in selected_indices if i in range(len(common_pool))]
            if not selected_columns:
                raise ValueError("No valid columns selected.")
        except ValueError as e:
            print(f"Invalid input: {e}")
            raise SystemExit

        # Create new surrogate_key column
        unique['surrogate_key'] = unique[selected_columns].fillna('').astype(str).agg(''.join, axis=1)

        # Move the new column to the first position
        columns = ['surrogate_key'] + [col for col in unique.columns if col not in ['surrogate_key']]
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
            if len(key_rows) == 2:  # Should be exactly 2 rows with same key
                row1_idx = key_rows.index[0]
                row2_idx = key_rows.index[1]
                
                # Skip columns that are metadata (surrogate_key, source, fail_column)
                data_columns = [col for col in unique.columns if col not in ['surrogate_key', 'source', 'fail_column']]
                
                # Find columns with different values
                differing_columns = []
                for col in data_columns:
                    if unique.loc[row1_idx, col] != unique.loc[row2_idx, col]:
                        differing_columns.append(col)
                
                # Set the failed columns for both rows
                if differing_columns:
                    fail_text = '| - |'.join(differing_columns)
                    unique.loc[row1_idx, 'fail_column'] = fail_text
                    unique.loc[row2_idx, 'fail_column'] = fail_text

        # Create a new field holding fail_column values and reorder columns
        unique.insert(2, 'failed_columns', unique['fail_column'])

        # Drop the old fail_column
        unique.drop(columns=['fail_column'], inplace=True)

        # Define the output filename
        output_filename = 'diff.csv'

        # Export the CSV
        unique.to_csv(output_filename, sep=',', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
        print("\n" + "-" * 50)
        print(f"Differences have been written to '{output_filename}'")
    else:
        print('No differences found.')

if __name__ == "__main__":
    main()
