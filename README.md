# py script to automate generating data validation results
- the scripts parses 2 .csv file with a number of common column names, prompts to select key field, then, if there are differences aka failed tests, generate a .csv that compares 2 files row-by-row, column-by-column using the keys. Else you are good to go!
- this should ideally help us quickly validate data, saving development time.

# installation
- `pip install pandas` - the only dependency.
- or use venv if you want to isolate the py env - `python -m venv .venv && source .venv/bin/activate`


# usage
- put the script and 2 csv files to validate in the same directory
- `python csv_diff_0.3.py`
- follow the prompt to select the files, and keys to be used
- the script will generate a new .csv file `diff.csv` with the results, if there are differences.

