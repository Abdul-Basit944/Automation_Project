import json
from fetch import fetch_ga4_data
from fetch_campaign_Gads import fetch_gads_data
from datetime import datetime
from collections import defaultdict

def clean_ga4_data_all_apps(config_file="apps_config.json"):
    with open(config_file) as f:
        configs = json.load(f)

    all_apps_monthly_data = {}

    for app_config in configs:
        app_name = app_config.get("app_name", "Unnamed")
        gads_data = fetch_gads_data(app_config)
        ga4_data = fetch_ga4_data(app_config, gads_data)

        if not ga4_data:
            print(f"No GA4 data fetched for {app_name}.")
            continue

        monthly_data = defaultdict(list)
        for row in ga4_data:
            date_obj = datetime.strptime(row['Formatted_Date'], '%d-%m-%Y')
            month_name = date_obj.strftime('%B')

            # Format monetary fields
            def fmt_money(val):
                return f"${round(float(val), 2)}" if isinstance(val, (int, float)) else "NA"

            for col in ['Gads_Spend', 'Total_spend', 'Total New Revenue', 'Ad Revenue', 'IAP_Revenue']:
                row[col] = fmt_money(row.get(col))

            for col in ['Count of Purchases', 'Renewal', 'Renewal_Count']:
                row[col] = int(row[col]) if isinstance(row[col], (int, float)) else 0

            for col in ['ROAS', 'ROI', 'L3_ROAS', 'L7_ROAS', 'L14_ROAS', 'L3_ROI', 'L7_ROI', 'L14_ROI']:
                row[col] = str(row[col]) if isinstance(row[col], (int, float)) else "N/A"

            for col in ['ROAS_Indicator', 'ROI_Indicator']:
                row[col] = str(row[col]) if row[col] is not None else "N/A"

            row['Total Revenue'] = row['Total New Revenue']
            row['__date_obj'] = date_obj

            monthly_data[month_name].append(row)

        all_apps_monthly_data[app_name] = monthly_data

    return all_apps_monthly_data

def print_cleaned_data_grouped_all_apps(all_apps_monthly_data):
    columns = [
        "Formatted_Date", "Gads_Spend", "Total_spend", "Total New Revenue",
        "Ad Revenue", "IAP_Revenue", "Count of Purchases", "Renewal",
        "Renewal_Count", "Total Revenue", "ROAS", "ROI", "L3_ROAS", 
        "L7_ROAS", "L14_ROAS", "L3_ROI", "L7_ROI", "L14_ROI", 
        "ROAS_Indicator", "ROI_Indicator"
    ]

    for app_name, monthly_data in all_apps_monthly_data.items():
        print(f"\n\n=== {app_name} ===")
        for month in sorted(monthly_data, key=lambda m: datetime.strptime(m, "%B").month):
            print(f"{month}\t" + "\t".join(columns[1:]))
            sorted_rows = sorted(monthly_data[month], key=lambda r: r['__date_obj'])
            for row in sorted_rows:
                values = [row.get(col, "") for col in columns]
                print("\t".join(str(v) for v in values))
            print("\n")

if __name__ == "__main__":
    all_data = clean_ga4_data_all_apps()
    if all_data:
        print_cleaned_data_grouped_all_apps(all_data)