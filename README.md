# IBKR-Flex-Demo
Demo project for processing data from IBKR to Power BI via Flex Reports

## Prerequisites
*   **Python 3.8 or higher.**
*   **Microsoft Power BI Desktop.**
*   **IBKR Flex Query Reports (XML format):** You will need to create Flex reports for regular update and history import. Refer to `flex_data_spec.md` for detailed column specifications.
    1.  **Trades:** Orders executions.
    2.  **Cash Transactions:** Deposits, dividends, interest, withholding tax, fees, etc.
    3.  **Positions:** Positions history and actual state of portfolio.
    4.  **Net Asset Value (NAV) in Base:** NAV history in base currency.
    5.  **Corporate Actions:** Details of splits, mergers, stock dividends, etc.

## Installation
1.  Clone repository
2.  Install Python and Power BI Desktop
3.  Run `python_venv_install.bat` to prepare virtual environment with necessary libraries
4.  Create flex reports on the Interactive Brokers site
5.  Download history files if necessary - see details below
6.  Update `config.yaml` configuration file with correct token, query ids and working folder
7.  Update `run_etl.bat` with correct working folder
8.  Run `import_history.bat` to upload history in database
9.  Run `run_etl.bat` to check if data loads normally
10.  Open `Report Template.pbit` and set working folder as parameter
11.  If data refresh fails - configure parameters of the report and run refresh again
12.  Save the report and now you can refresh it at any time

## Configuration
To correct processing of Python and files you need to change a couple of Power Bi settings - File / Options and Settings / Options
1. **GLOBAL / Python scripting / Set a Python home directory** : If you use .venv - point to `WORK FOlDER\IBKR_PBI_Kit\.venv\Scripts`, otherwise use the value detected by Power BI.
<img width="1002" height="802" alt="image" src="https://github.com/user-attachments/assets/5494e997-045d-4238-8de4-746c40b356aa" />

2. **CURRENT FILE / Data Load / Parallel loading** : Set to `One (disable parallel loading)`
<img width="1002" height="802" alt="image" src="https://github.com/user-attachments/assets/36dbf801-cddc-40d0-bcda-5a7869da3291" />

3. **CURRENT FILE / Privacy / Privacy Levels** : Set to `Ignore the Privacy Levels`
<img width="1002" height="802" alt="image" src="https://github.com/user-attachments/assets/5f06615f-55e0-460d-a727-f6846357c2ae" />  

## Preparing Input Data
To use Flex Queries you have to prepare your data for export.  
Before create Flex Queries please read documentation: https://www.ibkrguides.com/orgportal/performanceandstatements/flex.htm  
You'll need a token to access reports outside from original website: https://www.ibkrguides.com/orgportal/performanceandstatements/flex-web-service.htm  
Prepare five queries mentioned above. We recommend use 30-day period for each report - it will allow to get fresh data even if we skipped report refresh.  

## Import History Data
To import history you have to run each flex query manually for each year thet you want to upload.  
In the working directory create folder structure like next:  
`WORK FOlDER\history\cash`  
`WORK FOlDER\history\corporate_actions`  
`WORK FOlDER\history\nav`  
`WORK FOlDER\history\positions`  
`WORK FOlDER\history\trades`  
Save downloaded files into corresponding folder.  
