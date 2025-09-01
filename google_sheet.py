from cleaning import clean_ga4_data_all_apps, print_cleaned_data_grouped_all_apps
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import re
import traceback

def normalize_date(date_str):
    
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return date_str  

def append_all_apps_to_sheets(config_file="apps_config.json"):
    # Load all app configs
    with open(config_file) as f:
        configs = json.load(f)

    # Clean and group data for all apps
    all_apps_monthly_data = clean_ga4_data_all_apps(config_file)

    # Setup credentials scope
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

        # Get service account info from ga4.service_account_info
        service_account_info = app_config.get("ga4", {}).get("service_account_info", {})
        if not service_account_info or not service_account_info.get("private_key"):
            print(f"Missing or invalid service account info for app '{app_name}'. Skipping.")
            continue
        creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
        client = gspread.authorize(creds)

        # Get sheet info from sheets
        sheets_info = app_config.get("sheets", {})
        sheet_link = sheets_info.get("sheet_link")
        SHEET_ID = sheets_info.get("sheet_id")
        if not SHEET_ID and sheet_link:
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_link)
            if match:
                SHEET_ID = match.group(1)
        SHEET_NAME = sheets_info.get("sheet_name", app_name)
        if not SHEET_ID or not SHEET_NAME:
            print(f"Missing sheet_id or sheet_name for app '{app_name}'. Skipping.")
            continue
        try:
            worksheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        except Exception as e:
            print(f"Error opening sheet for app '{app_name}': {e}")
            traceback.print_exc()
            continue

        col_a = worksheet.col_values(1)
        existing_dates = set(
            normalize_date(str(cell).strip())
            for cell in col_a if "-" in str(cell)
        )
        existing_months = set(str(cell).strip() for cell in col_a if "-" not in str(cell) and str(cell).strip())
        last_row = len(col_a) + 1

        for month in sorted(monthly_data, key=lambda m: datetime.strptime(m, "%B").month):
            month_rows = monthly_data[month]
            new_rows = [
                row for row in sorted(
                    month_rows,
                    key=lambda r: datetime.strptime(r["Formatted_Date"], "%d-%m-%Y")
                )
                if normalize_date(row["Formatted_Date"]) not in existing_dates
            ]
            if not new_rows:
                continue
            if month not in existing_months:
                worksheet.append_row([
                    month,
                    "Gads_Spend", "Total_spend", "Total New Revenue",
                    "Ad Revenue", "IAP_Revenue", "Count of Purchases",
                    "Renewal", "Renewal_Count", "Total Revenue",
                    "ROAS", "ROI", "L3_ROAS", "L7_ROAS", "L14_ROAS",
                    "L3_ROI", "L7_ROI", "L14_ROI",
                    "ROAS_Indicator", "ROI_Indicator"
                ])
                last_row += 1
            for row in new_rows:
                row_data = [
                    row["Formatted_Date"],
                    row["Gads_Spend"],
                    row["Total_spend"],
                    row["Total New Revenue"],
                    row["Ad Revenue"],
                    row["IAP_Revenue"],
                    row["Count of Purchases"],
                    row["Renewal"],
                    row["Renewal_Count"],
                    row["Total Revenue"],
                    row["ROAS"],
                    row["ROI"],
                    row["L3_ROAS"],
                    row["L7_ROAS"],
                    row["L14_ROAS"],
                    row["L3_ROI"],
                    row["L7_ROI"],
                    row["L14_ROI"],
                    row["ROAS_Indicator"],
                    row["ROI_Indicator"]
                ]
                worksheet.append_row(row_data)
                last_row += 1
            for _ in range(3):
                worksheet.append_row([""] * 21)
                last_row += 1
            existing_months.add(month)
        print(f"New data appended to '{SHEET_NAME}' tab for app '{app_name}' successfully.")

if __name__ == "__main__":
    append_all_apps_to_sheets()
