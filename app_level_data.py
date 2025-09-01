import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import re
import traceback
from cleaning import clean_ga4_data_all_apps

def format_currency(value):
    try:
        return f"${float(value):.2f}"
    except (ValueError, TypeError):
        return value if value else ""

def format_percent(value):
    try:
        value_str = str(value)
        if value_str.startswith(("+", "-")):
            return value_str
        return f"+{float(value):.1f}%"
    except (ValueError, TypeError):
        return value if value else ""

def append_new_unique_rows_all_apps(config_file="apps_config.json"):
    with open(config_file) as f:
        configs = json.load(f)

    all_apps_monthly_data = clean_ga4_data_all_apps(config_file)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    for app_config in configs:
        app_name = app_config.get("app_name", "Unnamed")
        monthly_data = all_apps_monthly_data.get(app_name)
        if not monthly_data:
            print(f"No data to write for {app_name}.")
            continue

        # Prefer app-level sheet if available, else fallback to warehouse sheet
        sheets_info = app_config.get("app_sheet") or app_config.get("sheets", {})
        sheet_link = sheets_info.get("sheet_link")
        SHEET_ID = sheets_info.get("sheet_id")
        if not SHEET_ID and sheet_link:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_link)
            if match:
                SHEET_ID = match.group(1)
        SHEET_NAME = sheets_info.get("sheet_name")
        if not SHEET_ID or not SHEET_NAME:
            print(f"Missing sheet_id or sheet_name for app '{app_name}'. Skipping.")
            continue
        print(f"Writing to sheet: {SHEET_ID}, tab: {SHEET_NAME} for app: {app_name}")

        # Use per-app service account info
        service_account_info = app_config.get("service_account_info") or app_config.get("ga4", {}).get("service_account_info")
        if not service_account_info:
            print(f"Missing service_account_info for app '{app_name}'. Skipping.")
            continue
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)

        try:
            worksheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        except Exception as e:
            print(f"Error opening sheet for app '{app_name}': {e}")
            traceback.print_exc()
            continue

        col_a = worksheet.col_values(1)
        # Only consider real date rows, skip month headers and blanks
        existing_dates = set(
            str(cell).strip()
            for cell in col_a
            if "-" in str(cell) and str(cell).strip() not in ["", None] and not str(cell).strip().isalpha()
        )
        existing_months = set(str(cell).strip() for cell in col_a if "-" not in str(cell) and str(cell).strip())
        last_row = len(col_a) + 1

        headers = [
            "Gads_Spend", "Total_spend", "Total New Revenue", "Ad Revenue", "IAP_Revenue",
            "Count of Purchases", "Renewal", "Renewal_Count", "Total Revenue",
            "ROAS", "ROI", "L3_ROAS", "L7_ROAS", "L14_ROAS",
            "L3_ROI", "L7_ROI", "L14_ROI", "ROAS_Indicator", "ROI_Indicator"
        ]

        all_rows = []
        for month in sorted(monthly_data, key=lambda m: datetime.strptime(m, "%B").month):
            month_rows = monthly_data[month]
            new_month_rows = [row for row in month_rows if row["Formatted_Date"] not in existing_dates]
            if not new_month_rows:
                continue
            if month not in existing_months:
                all_rows.append([month] + headers)
            new_month_rows.sort(key=lambda r: datetime.strptime(r["Formatted_Date"], "%d-%m-%Y"))
            for row in new_month_rows:
                row_data = [
                    row["Formatted_Date"],
                    format_currency(row["Gads_Spend"]),
                    format_currency(row["Total_spend"]),
                    format_currency(row["Total New Revenue"]),
                    format_currency(row["Ad Revenue"]),
                    format_currency(row["IAP_Revenue"]),
                    row["Count of Purchases"],
                    row["Renewal"],
                    row["Renewal_Count"],
                    format_currency(row["Total Revenue"]),
                    row["ROAS"],
                    row["ROI"],
                    row["L3_ROAS"],
                    row["L7_ROAS"],
                    row["L14_ROAS"],
                    row["L3_ROI"],
                    row["L7_ROI"],
                    row["L14_ROI"],
                    format_percent(row["ROAS_Indicator"]),
                    format_percent(row["ROI_Indicator"])
                ]
                all_rows.append(row_data)
            all_rows.extend([[""] * 21 for _ in range(3)])  # blank rows after each month

        if all_rows:
            worksheet.append_rows(all_rows)
            data_row_count = sum(1 for row in all_rows if any(str(cell).strip() for cell in row))
            print(f"Appended {data_row_count} data row(s) for app '{app_name}', sorted and structured by month.")
        else:
            print(f"No new data to append. All dates already exist in sheet.")

if __name__ == "__main__":
    append_new_unique_rows_all_apps()
