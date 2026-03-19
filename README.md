# IBKR-Flex-Demo

Demo project for importing Interactive Brokers Flex Reports into a local analytical store and building a Power BI report on top of it.

This project is intended for **Windows + Power BI Desktop** usage and uses **Python scripts + local files + Power BI template** as the main workflow.

---

## What this project does

The project helps you:

- download and process data from **IBKR Flex Queries**
- import historical XML files into a local database/cache
- run ETL refreshes for regular updates
- open a ready-to-use **Power BI template**
- analyze portfolio activity, positions, NAV, cash flows, trades, and corporate actions

---

## Prerequisites

Before starting, make sure you have:

- **Windows**
- **Python 3.8+**
- **Microsoft Power BI Desktop**
- **Interactive Brokers account access**
- **IBKR Flex Queries in XML format**
- **Flex Web Service token**

This project expects the following Flex report types:

1. **Trades** — order executions and trading history  
2. **Cash Transactions** — deposits, withdrawals, dividends, interest, taxes, fees, etc.  
3. **Positions** — position history and current portfolio state  
4. **Net Asset Value (NAV) in Base** — portfolio NAV history in base currency  
5. **Corporate Actions** — splits, mergers, stock dividends, symbol changes, etc.  

Refer to `flex_data_spec.md` for the expected fields and column details.

---

## Quick Start

1. Clone this repository.
2. Install Python and Power BI Desktop.
3. Run `python_venv_install.bat` to create a local virtual environment and install dependencies.
4. Create the required IBKR Flex Queries.
5. Generate a Flex Web Service token in IBKR.
6. Update `config.yaml` with:
   - your token
   - your query IDs
   - your working folder
7. Update `run_etl.bat` with the correct working folder if needed.
8. If you want historical data, manually export yearly XML files and place them into the `history` subfolders.
9. Run `import_all.bat` (or the individual import `.bat` files) to load historical XML files.
10. Run `run_etl.bat` to load or refresh current data.
11. Open `Report Template.pbit`.
12. Set the report parameters, including the working folder.
13. Refresh the report in Power BI.
14. Save the report as a `.pbix` file for future use.

---

## Repository Workflow

There are three main stages in the workflow:

### 1. Historical import
Used when you already have exported XML files for previous periods (for example, yearly history exports).

### 2. ETL refresh
Used to load recent data from IBKR Flex Web Service using your token and query IDs.

### 3. Power BI reporting
Used to visualize imported and refreshed data through the provided Power BI template.

---

## Configuration

### `config.yaml`

Update `config.yaml` with your real settings:

- **Flex Web Service token**
- **query IDs for each Flex report**
- **working folder path**

Example structure:

```yaml
working_folder: D:\WORK\IBKR-Flex-Demo

ibkr:
  token: YOUR_FLEX_TOKEN
  trades_query_id: YOUR_TRADES_QUERY_ID
  cash_query_id: YOUR_CASH_QUERY_ID
  positions_query_id: YOUR_POSITIONS_QUERY_ID
  nav_query_id: YOUR_NAV_QUERY_ID
  corporate_actions_query_id: YOUR_CORPORATE_ACTIONS_QUERY_ID
```

Adjust the exact keys to match the actual file structure used in this repository.

### `run_etl.bat`

Check `run_etl.bat` and make sure the working directory or project path matches your local installation.

---

## Power BI Configuration

To make Power BI work correctly with Python and local files, adjust these settings:

### 1. Python home directory
Go to:

**File → Options and settings → Options → Global → Python scripting**

If you are using the project virtual environment, point Power BI to:

```text
YOUR_PROJECT_FOLDER\.venv\Scripts
```

Example:

```text
D:\WORK\IBKR-Flex-Demo\.venv\Scripts
```

Do not use an unrelated project path.

### 2. Disable parallel loading
Go to:

**File → Options and settings → Options → Current File → Data Load**

Set:

```text
Parallel loading = One (disable parallel loading)
```

### 3. Ignore privacy levels
Go to:

**File → Options and settings → Options → Current File → Privacy**

Set:

```text
Ignore the Privacy Levels
```

These settings are important because the report combines Python execution and local file access, and Power BI can otherwise block or break refresh behavior.

---

## Preparing IBKR Flex Queries

Before creating the Flex Queries, read the IBKR documentation:

- Flex statements overview
- Flex Web Service token setup

You need to prepare the five query types listed above.

### Recommendation
Use a **30-day period** for regular update queries. This gives the ETL some overlap and helps avoid gaps if one or more scheduled refreshes are missed. This recommendation is already reflected in the current project documentation.

---

## Importing Historical Data

If you want to load historical data, export XML files manually from IBKR for each year or period you want to import.

Create the following folder structure inside your working folder:

```text
YOUR_WORKING_FOLDER/
  history/
    cash/
    corporate_actions/
    nav/
    positions/
    trades/
```

Example:

```text
D:\WORK\IBKR-Flex-Demo\
  history\
    cash\
    corporate_actions\
    nav\
    positions\
    trades\
```

Save the downloaded XML files into the corresponding folders.

### Import scripts

Depending on your setup, use either:

- `import_all.bat` for all history imports
- or individual scripts such as:
  - `import_cash_transactions.bat`
  - `import_corporate_actions.bat`
  - `import_nav.bat`
  - `import_positions.bat`
  - `import_trades.bat`

Use the script names that actually exist in the repository.

---

## Regular Refresh

After configuration is complete:

1. Run `run_etl.bat`
2. Verify that data loads without errors
3. Open `Report Template.pbit`
4. Provide the required parameters
5. Refresh the report

If everything works correctly, save the result as a `.pbix` file and use it for future refreshes.

---

## Expected Happy Path

A normal first-time setup usually looks like this:

1. Install software
2. Clone repo
3. Create `.venv`
4. Configure `config.yaml`
5. Export and import historical XML files
6. Run ETL
7. Open Power BI template
8. Set report parameters
9. Refresh report
10. Save `.pbix`

---

## Troubleshooting

### Power BI cannot find Python
Check the Python scripting path in Power BI settings.  
If you use the virtual environment, it should point to:

```text
YOUR_PROJECT_FOLDER\.venv\Scripts
```

### Report refresh fails in Power BI
Check:

- working folder parameter
- Python path in Power BI
- privacy settings
- parallel loading setting
- whether the ETL already produced the expected files/data

### ETL runs but report still fails
Re-open the template and verify all parameters again.  
Power BI sometimes keeps stale parameter values or old local paths.

### Historical import does not find files
Make sure:

- the folder structure is correct
- files are placed in the matching subfolder
- XML files were exported in the expected Flex format

### Flex Web Service does not return data
Check:

- token validity
- query ID correctness
- whether the query is enabled and accessible in IBKR
- whether the selected date range actually contains data

---

## Notes

- This project is primarily oriented toward **local desktop usage**
- The provided automation scripts are **Windows `.bat` scripts**
- Historical data import and regular ETL refresh are separate steps
- Historical XML export from IBKR is still a manual step

---

## Suggested First Improvements for This Repository

If you continue refining this project, the most useful documentation improvements would be:

- add a real `config.yaml` sample file
- add screenshots for Flex Query creation in IBKR
- document expected ETL output files/tables
- document which report pages depend on which source datasets
- add a dedicated troubleshooting section for common Power BI issues

---

## References

- `README.md`
- `flex_data_spec.md`
- IBKR Flex Statements documentation
- IBKR Flex Web Service documentation
