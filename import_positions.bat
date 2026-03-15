@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

for /f "delims=" %%F in ('dir /b /s /o:n ".\history\positions\*.xml"') do (
    echo.
    echo Importing: %%~nxF

    .\.venv\Scripts\python -m scripts.tools.import_positions_xml ^
        --duckdb ".\cache\warehouse.duckdb" ^
        --file "%%F"

    if errorlevel 1 (
        echo ERROR importing %%F
        pause
        exit /b 1
    )
)
