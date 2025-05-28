# 📊 minimal-csv-diff

A minimal tool to compare CSV files and generate diff reports for data validation.

## ✨ Features

- 🔍 Compare two CSV files with common column names
- 🎯 Interactive selection of key fields for comparison
- 📋 Generate detailed diff reports when differences are found
- ⚡ Command-line interface for quick data validation
- 🔎 Identifies unique rows and column-level differences
- 📁 Exports results to CSV format for further analysis

## 🚀 Quick Start

### Option 1: Run Instantly (No Installation) ⭐

```bash
uvx minimal-csv-diff
```

### Option 2: Install & Run

```bash
pip install minimal-csv-diff
minimal-csv-diff
```

## 🎮 Try the Demo

Want to see it in action? Check out the [demo](demo/demo.md) directory:

```bash
cd demo/
minimal-csv-diff
# Follow prompts: select files 0,1 and choose a key column
# See the magic happen! ✨
```

The demo includes sample CSV files and shows how the tool identifies:
- 🔴 **Unique rows** (exist in only one file)
- 🟡 **Column differences** (same record, different values)
- ✅ **Matching records** (excluded from output)

## 📖 How It Works

1. **📂 Select directory** containing your CSV files
2. **⚙️ Choose delimiter** (comma, semicolon, etc.)
3. **📄 Pick two files** to compare
4. **🔑 Select key columns** for row matching
5. **📊 Get diff.csv report** if differences exist

## 📤 Output

When differences are found, generates a `diff.csv` with:

- **🔑 surrogate_key**: Concatenated key fields for row identification
- **📁 source**: Which file the row comes from
- **❌ failed_columns**: Which columns differ or "UNIQUE ROW"
- **📋 All original columns**: Complete data for comparison

## 💡 Use Cases

- **🔄 Data validation** between different data sources
- **🔧 ETL pipeline testing** - compare before/after transformations
- **🗄️ Database migration verification** - ensure data integrity
- **📊 Looker dashboard validation** - compare query results across environments
- **🧪 A/B testing data analysis** - identify differences in datasets

## 🛠️ Development

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
git clone https://github.com/joon-solutions/looker_data_validation
cd looker_data_validation
uv sync
uv run minimal-csv-diff
```

## 📋 Requirements

- Python >= 3.10
- pandas >= 2.0.0