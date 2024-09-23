# Usage 
This script compares the values of the 2 CSV files using specified keys and matched column names (note: make sure column names are identical between these files).

# Prerequisites
2 CSV files with 
- Matching column names (proper ordering is not required)
- Columns that used as surrogate key / PK

# Getting started
Getting started involves (in this order):
1. Clone or download a copy of this repository to your development machine:
```
pip install --upgrade git+https://github.com/joon-solutions/looker_data_validation.git
```

2. In your terminal:
- Go to the directory with the CSV files using `cd`
- Run `csv-diff` and follow the prompts
  - Enter the indices of the two files to compare, separated by a comma
    ![image](https://github.com/user-attachments/assets/aa0df05b-1905-422f-9e35-ca661eca6b9c)
  - Select (in order) a surrogate key / PK to order the results.
    ![image](https://github.com/user-attachments/assets/18235951-3624-4553-9905-b1071e0b23b9)

- The script will generate a new `diff.csv` file with the results, if there are differences.
  ![image](https://github.com/user-attachments/assets/e8a7b48d-6992-4399-bb77-69c3220e4202)
