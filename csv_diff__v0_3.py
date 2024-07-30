#%%
import os
import pandas as pd
import csv

def main():
#%%
 
    foreign_dir = os.environ.get("CSV_DIFF_INSTALLED","0")

    if foreign_dir == "1":
        workdir = os.getcwd()
    elif foreign_dir == "0":
        workdir = os.path.dirname(__file__)

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
        file1_index = int(input("Enter the index of the first file to compare: "))
        file2_index = int(input("Enter the index of the second file to compare: "))
        if file1_index not in range(len(csv_files)) or file2_index not in range(len(csv_files)) or file1_index == file2_index:
            raise ValueError("Invalid indices or indices are the same.")
    except ValueError as e:
        print(f"Invalid input: {e}")
        raise SystemExit

    print("\n" + "-" * 50)  # Prints a line of 50 dashes

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
    column_pool = list(set(column_pool))  # Remove duplicates

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
        
        print("\n" + "-" * 50)  # Prints a line of 50 dashes
        
        unique['source'] = unique['_merge'].apply(flag_column)
        
        # Clone '_merge' column to a new column
        unique['result'] = unique['_merge']
        
        # Reorder columns to place 'source' and 'result' at the start
        columns = ['source', 'result'] + [col for col in unique.columns if col not in ['source', 'result']]
        unique = unique[columns]

        print("\nSelect (in order) a surrogate key / PK to order the results.\nAvailable columns for concatenation:")
        print("\n" + "-" * 50)  # Prints a line of 50 dashes

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

        # Debugging: Print column names and a few rows
        # print(f"Columns in DataFrame: {unique.columns}")
        # print(unique.head())

        # Initialize the fail_column with empty values
        unique['fail_column'] = ''

        # Case 1: Find consecutive rows with the same surrogate_key but different source
        for i in range(len(unique) - 1):
            if (unique.loc[i, 'surrogate_key'] == unique.loc[i + 1, 'surrogate_key'] and
                unique.loc[i, 'source'] != unique.loc[i + 1, 'source']):
                
                # Identify columns with different values
                differing_columns = [col for col in unique.columns[2:] if unique.loc[i, col] != unique.loc[i + 1, col]]
                
                # Append differing columns to fail_column
                unique.loc[i, 'fail_column'] = '| - |'.join(differing_columns)
                unique.loc[i + 1, 'fail_column'] = '| - |'.join(differing_columns)

        # Case 2: Find consecutive rows with the same source but different surrogate_key
        for i in range(len(unique) - 1):
            if (unique.loc[i, 'source'] == unique.loc[i + 1, 'source'] and
                unique.loc[i, 'surrogate_key'] != unique.loc[i + 1, 'surrogate_key']):
                
                unique.loc[i, 'fail_column'] = 'UNIQUE ROW'
                unique.loc[i + 1, 'fail_column'] = 'UNIQUE ROW'

        # Ensure both rows with the same surrogate_key have consistent fail_column values
        for i in range(len(unique) - 1):
            if (unique.loc[i, 'surrogate_key'] == unique.loc[i + 1, 'surrogate_key']):
                if unique.loc[i, 'fail_column'] != 'UNIQUE ROW' and unique.loc[i + 1, 'fail_column'] == 'UNIQUE ROW':
                    unique.loc[i + 1, 'fail_column'] = unique.loc[i, 'fail_column']
                elif unique.loc[i, 'fail_column'] == 'UNIQUE ROW' and unique.loc[i + 1, 'fail_column'] != 'UNIQUE ROW':
                    unique.loc[i, 'fail_column'] = unique.loc[i + 1, 'fail_column']

        # Create a new field holding fail_column values and reorder columns
        unique.insert(2, 'failed_columns', unique['fail_column'])

        # Drop the old fail_column
        unique.drop(columns=['fail_column'], inplace=True)

        # Define the output filename
        output_filename = 'diff.csv'

        # Export the CSV
        unique.to_csv(output_filename, sep=',', index=False, quotechar='"', quoting=csv.QUOTE_ALL)

        print("\n" + "-" * 50)  # Prints a line of 50 dashes

        # Print output filename
        print(f"Differences have been written to {output_filename}")

    else:
        print('No differences found.')

#%%

# %%

if __name__ == "__main__":
    main()