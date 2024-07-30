# py script to automate generating data validation results

- the scripts parses 2 .csv file with a number of common column names, prompts to select key field, then, if there are differences aka failed tests, generate a .csv that compares 2 files row-by-row, column-by-column using the keys. Else you are good to go!
- this should ideally help us quickly validate data, saving development time.
- once installed, the script can be invoked either from terminal or imported as a package.

# installation

`pip install --upgrade git+https://github.com/joon-solutions/looker_data_validation.git`

# usage

**from terminal**

- cd to the directory with the csv files.
- run `csv-diff` and follow the prompts
  - select the index of 2 files to compare
  - select the columns to be used as surrogate key
- the script will generate a new .csv file `diff.csv` with the results, if there are differences.

**from a python script**

* WIP

![image](https://github.com/user-attachments/assets/8929693a-ed2b-489a-862c-631c82d1b89a)
