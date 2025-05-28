## 🚀 Demo: minimal-csv-diff in Action

```bash
# Install the package
pip install minimal-csv-diff
```

```bash
# Or run directly with uvx (no installation needed)
uvx minimal-csv-diff
```

```
Workdir is "/Users/luutuankiet/tmp/looker_data_validation".
Enter to confirm or input the full path to the directory containing the CSV files to compare:
> /Users/luutuankiet/tmp/looker_data_validation

current workdir is :/Users/luutuankiet/tmp/looker_data_validation/demo
input the file delimiter:
> ,

Available CSV files:
0: file1.csv
1: file2.csv

Enter the indices of the two files to compare, separated by a comma:
> 0,1

--------------------------------------------------
Found differences between the two files.

--------------------------------------------------

Select (in order) a surrogate key / PK to order the results.
Available columns for concatenation:

--------------------------------------------------
0: age
1: id
2: name
3: city

Enter indices of columns to concatenate (comma-separated): 0

--------------------------------------------------
Differences have been written to 'diff.csv'
```

### 📄 Sample Input Files

demo/file1.csv
| id | name    | age | city        |
|----|---------|-----|-------------|
| 1  | Alice   | 30  | New York    |
| 2  | Bob     | 25  | Los Angeles |
| 3  | Charlie | 35  | Chicago     |
| 4  | Diana   | 28  | Houston     |

demo/file2.csv
| id | name    | age | city          |
|----|---------|-----|---------------|
| 1  | Alice   | 30  | New York      |
| 2  | Bob     | 26  | Los Angeles   |
| 3  | Charlie | 35  | San Francisco |
| 5  | Eve     | 22  | Austin        |


### 📄 Generated `diff.csv` Output

| surrogate_key | source    | failed_columns | name    | id  | city          | age |
| ------------- | --------- | -------------- | ------- | --- | ------------- | --- |
| 2             | file1.csv | age            | Bob     | 2   | Los Angeles   | 25  |
| 2             | file2.csv | age            | Bob     | 2   | Los Angeles   | 26  |
| 3             | file1.csv | city           | Charlie | 3   | Chicago       | 35  |
| 3             | file2.csv | city           | Charlie | 3   | San Francisco | 35  |
| 4             | file1.csv | UNIQUE ROW     | Diana   | 4   | Houston       | 28  |
| 5             | file2.csv | UNIQUE ROW     | Eve     | 5   | Austin        | 22  |


### 🔎 Key Highlights

- **Eve** (age 22) and **Diana** (age 28) are **unique rows** - exist only in one file
- **Bob's age** differs: 25 in file1 vs 26 in file2
- **Charlie's city** differs: Chicago in file1 vs San Francisco in file2
- **Alice** has no differences (not shown in diff output)