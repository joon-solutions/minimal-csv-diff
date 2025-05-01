## 🚀 Demo: CSV Diff in Action

```bash
# make sure you installed the package first
uv run csv-diff
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

### 📄 `diff.csv` Output

```csv
"surrogate_key","source","failed_columns","age","id","name","city"
"22","file2.csv","","22","5","Eve","Austin"
"25","file1.csv","","25","2","Bob","Los Angeles"
"26","file2.csv","","26","2","Bob","Los Angeles"
"28","file1.csv","UNIQUE ROW","28","4","Diana","Houston"
"35","file1.csv","city","35","3","Charlie","Chicago"
"35","file2.csv","city","35","3","Charlie","San Francisco"
```

### 🔎 Key Highlights

- `Eve` and `Diana` are **unique rows** (exist only in one file).
- `Charlie`'s `city` value mismatches across files.
- `Bob`'s `age` differs between file1 and file2.
