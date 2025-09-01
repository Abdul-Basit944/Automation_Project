import json
import base64
import tempfile
import os
from datetime import datetime, timedelta
from collections import defaultdict
from fetch_campaign_Gads import fetch_gads_data
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

def fix_base64_padding(data):
    if isinstance(data, str):
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
    return data

def validate_and_fix_service_account_info(ga4_config):
    try:
        if isinstance(ga4_config, str):
            with open(ga4_config, 'r') as f:
                service_account_info = json.load(f)
        else:
            service_account_info = ga4_config.copy()

        if 'private_key' in service_account_info:
            private_key = service_account_info['private_key']

            if private_key.startswith('-----BEGIN PRIVATE KEY-----'):
                private_key = private_key.replace('\\n', '\n')
                lines = private_key.split('\n')
                cleaned_lines = [line.strip() for line in lines if line.strip()]
                private_key = '\n'.join(cleaned_lines)

                # Ensure footer
                if not private_key.endswith('-----END PRIVATE KEY-----'):
                    private_key += '\n-----END PRIVATE KEY-----'
                if not private_key.endswith('\n'):
                    private_key += '\n'

                service_account_info['private_key'] = private_key
                print("Fixed private key formatting")
            else:
                print("Warning: Private key doesn't start with expected header")

        return service_account_info

    except Exception as e:
        print(f"Error validating service account info: {e}")
        raise

def fetch_ga4_data(app_config, gads_data=None):
    try:
        ga4_config = app_config["ga4"]
        service_account_info = ga4_config["service_account_info"]
        PROPERTY_ID = ga4_config["property_id"]
        client = BetaAnalyticsDataClient.from_service_account_info(service_account_info)

        today = datetime.today()
        start_date = today - timedelta(days=16)
        end_date = today - timedelta(days=1)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="totalRevenue"),
                Metric(name="purchaseRevenue"),
                Metric(name="transactions")
            ],
            date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)]
        )
        response = client.run_report(request)

        renewal_request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="totalUsers")
            ],
            date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
            dimension_filter={
                'filter': {
                    'field_name': 'eventName',
                    'string_filter': {'value': 'purchase'}
                }
            }
        )
        renewal_response = client.run_report(renewal_request)

        renewal_data = {}
        for row in renewal_response.rows:
            raw_date = row.dimension_values[0].value
            date_str = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m-%d")
            renewal_count = int(row.metric_values[0].value or 0)
            renewal = int(row.metric_values[1].value or 0)
            renewal_data[date_str] = {
                "Renewal": renewal,
                "Renewal_Count": renewal_count
            }

        # ✅ Create a set of all dates we need to process (from both GA4 and Google Ads)
        all_dates = set()
        gads_data = gads_data or {}
        
        # Add dates from GA4 data
        for row in response.rows:
            raw_date = row.dimension_values[0].value
            date_obj = datetime.strptime(raw_date, "%Y%m%d")
            all_dates.add(date_obj)
        
        # Add dates from Google Ads data
        for date_str in gads_data.keys():
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            all_dates.add(date_obj)
        
        # If no data from either source, create date range
        if not all_dates:
            current_date = start_date
            while current_date <= end_date:
                all_dates.add(current_date)
                current_date += timedelta(days=1)

        # ✅ Create GA4 data lookup
        ga4_data_lookup = {}
        for row in response.rows:
            raw_date = row.dimension_values[0].value
            date_obj = datetime.strptime(raw_date, "%Y%m%d")
            date_str = date_obj.strftime("%Y-%m-%d")
            
            total_revenue = float(row.metric_values[0].value or 0.0)
            iap_revenue = float(row.metric_values[1].value or 0.0)
            count_of_purchases = int(row.metric_values[2].value or 0)
            
            ga4_data_lookup[date_str] = {
                "total_revenue": total_revenue,
                "iap_revenue": iap_revenue,
                "count_of_purchases": count_of_purchases
            }

        all_data = []

        # ✅ Process all dates (both GA4 and Google Ads dates)
        for date_obj in sorted(all_dates):
            date_str = date_obj.strftime("%Y-%m-%d")
            formatted_date = f"{date_obj.day}-{date_obj.month}-{date_obj.year}"

            # Get GA4 data (or use zeros if not available)
            ga4_row = ga4_data_lookup.get(date_str, {
                "total_revenue": 0.0,
                "iap_revenue": 0.0,
                "count_of_purchases": 0
            })
            
            total_revenue = ga4_row["total_revenue"]
            iap_revenue = ga4_row["iap_revenue"]
            count_of_purchases = ga4_row["count_of_purchases"]

            # Get renewal data
            renewal = renewal_data.get(date_str, {}).get("Renewal", 0)
            renewal_count = renewal_data.get(date_str, {}).get("Renewal_Count", 0)

            # ✅ Get Google Ads spend (preserve actual values, don't default to 0)
            gads_spend = gads_data.get(date_str, 0.0)
            total_spend = gads_spend if gads_spend is not None else 0.0

            ad_revenue = total_revenue - iap_revenue
            roas = round(total_revenue / total_spend, 2) if total_spend and total_spend > 0 else 0
            roi = round((total_revenue - total_spend) / total_spend, 2) if total_spend and total_spend > 0 else 0

            # ✅ Add row with all data (including Google Ads spend)
            all_data.append({
                "date_obj": date_obj,
                "date_str": date_str,
                "Formatted_Date": formatted_date,
                "Month": date_obj.strftime("%B"),
                "Gads_Spend": gads_spend if gads_spend else 0.0,
                "Total_spend": total_spend if total_spend else 0.0,
                "Total New Revenue": total_revenue if total_revenue else 0.0,
                "Ad Revenue": ad_revenue if ad_revenue else 0.0,
                "IAP_Revenue": iap_revenue if iap_revenue else 0.0,
                "Count of Purchases": count_of_purchases if count_of_purchases else 0,
                "Renewal": renewal if renewal else 0,
                "Renewal_Count": renewal_count if renewal_count else 0,
                "ROAS": roas if roas else 0,
                "ROI": roi if roi else 0
            })

        all_data.sort(key=lambda x: x["date_obj"])

        for i, row in enumerate(all_data):
            l3_roas, l7_roas, l14_roas = calculate_averages(all_data, i, "ROAS")
            l3_roi, l7_roi, l14_roi = calculate_averages(all_data, i, "ROI")
            roas_indicator = calculate_simple_indicator(all_data, i, "ROAS")
            roi_indicator = calculate_simple_indicator(all_data, i, "ROI")
            row.update({
                "L3_ROAS": l3_roas,
                "L7_ROAS": l7_roas,
                "L14_ROAS": l14_roas,
                "L3_ROI": l3_roi,
                "L7_ROI": l7_roi,
                "L14_ROI": l14_roi,
                "ROAS_Indicator": roas_indicator,
                "ROI_Indicator": roi_indicator
            })

        display_start_date = today - timedelta(days=2)
        display_end_date = today - timedelta(days=1)

        enhanced_data = [row for row in all_data if display_start_date <= row["date_obj"] <= display_end_date]

        # ✅ If no rows in display range, create them but preserve Google Ads data
        if not enhanced_data:
            for i in range(2, 0, -1):
                d = today - timedelta(days=i)
                date_str = d.strftime("%Y-%m-%d")
                
                # ✅ Check if there's Google Ads data for this date
                gads_spend_for_date = gads_data.get(date_str, 0.0)
                
                enhanced_data.append({
                    "date_obj": d,
                    "date_str": date_str,
                    "Formatted_Date": f"{d.day}-{d.month}-{d.year}",
                    "Month": d.strftime("%B"),
                    "Gads_Spend": gads_spend_for_date,  # ✅ Preserve actual Google Ads spend
                    "Total_spend": gads_spend_for_date,  # ✅ Preserve actual Google Ads spend
                    "Total New Revenue": 0.0,
                    "Ad Revenue": 0.0,
                    "IAP_Revenue": 0.0,
                    "Count of Purchases": 0,
                    "Renewal": 0,
                    "Renewal_Count": 0,
                    "ROAS": 0,
                    "ROI": 0,
                    "L3_ROAS": 0,
                    "L7_ROAS": 0,
                    "L14_ROAS": 0,
                    "L3_ROI": 0,
                    "L7_ROI": 0,
                    "L14_ROI": 0,
                    "ROAS_Indicator": "N/A",
                    "ROI_Indicator": "N/A"
                })

        return enhanced_data

    except Exception as e:
        print(f"Error in fetch_ga4_data: {e}")
        
        # ✅ Even when GA4 fails, return data with Google Ads spend if available
        if gads_data:
            print(f"GA4 failed but Google Ads data available, creating fallback data...")
            today = datetime.today()
            fallback_data = []
            
            display_start_date = today - timedelta(days=2)
            display_end_date = today - timedelta(days=1)
            
            current_date = display_start_date
            while current_date <= display_end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                gads_spend_for_date = gads_data.get(date_str, 0.0)
                
                fallback_data.append({
                    "date_obj": current_date,
                    "date_str": date_str,
                    "Formatted_Date": f"{current_date.day}-{current_date.month}-{current_date.year}",
                    "Month": current_date.strftime("%B"),
                    "Gads_Spend": gads_spend_for_date,
                    "Total_spend": gads_spend_for_date,
                    "Total New Revenue": 0.0,
                    "Ad Revenue": 0.0,
                    "IAP_Revenue": 0.0,
                    "Count of Purchases": 0,
                    "Renewal": 0,
                    "Renewal_Count": 0,
                    "ROAS": 0,
                    "ROI": 0,
                    "L3_ROAS": 0,
                    "L7_ROAS": 0,
                    "L14_ROAS": 0,
                    "L3_ROI": 0,
                    "L7_ROI": 0,
                    "L14_ROI": 0,
                    "ROAS_Indicator": "N/A",
                    "ROI_Indicator": "N/A"
                })
                
                current_date += timedelta(days=1)
            
            return fallback_data
        
        return []


def calculate_averages(all_data, current_index, metric):
    def get_average(days):
        if current_index < days - 1:
            return None
        values = [
            all_data[j][metric] for j in range(current_index - days + 1, current_index + 1)
            if all_data[j][metric] is not None
        ]
        return round(sum(values) / len(values), 2) if values else None
    return get_average(3), get_average(7), get_average(14)

def calculate_simple_indicator(all_data, current_index, metric):
    if current_index < 1:
        return "0%"
    current_value = all_data[current_index][metric]
    previous_value = all_data[current_index - 1][metric]
    if current_value is None or previous_value is None or previous_value == 0:
        return "N/A"
    change_percent = ((current_value - previous_value) / previous_value) * 100
    if change_percent == 0:
        return "0%"
    elif change_percent > 0:
        return f"+{change_percent:.1f}%"
    else:
        return f"{change_percent:.1f}%"

def print_monthly_report(data):
    monthly_data = defaultdict(list)
    for row in sorted(data, key=lambda x: datetime.strptime(x["Formatted_Date"], "%d-%m-%Y")):
        monthly_data[row["Month"]].append(row)

    for month, rows in monthly_data.items():
        print(f"{month}\tGads_Spend\tTotal_spend\tTotal New Revenue\tAd Revenue\tIAP_Revenue\tCount of Purchases\tRenewal\tRenewal_Count\tTotal Revenue\tROAS\tROI\tL3_ROAS\tL7_ROAS\tL14_ROAS\tL3_ROI\tL7_ROI\tL14_ROI\tROAS_Indicator\tROI_Indicator")
        for row in rows:
            def money(val): return f"${val:.2f}" if isinstance(val, (float, int)) else "N/A"
            def format_val(val): return f"{val:.2f}" if isinstance(val, (float, int)) else "N/A"
            print(f"{row['Formatted_Date']}\t{money(row['Gads_Spend'])}\t{money(row['Total_spend'])}\t{money(row['Total New Revenue'])}\t{money(row['Ad Revenue'])}\t{money(row['IAP_Revenue'])}\t{row['Count of Purchases']}\t{row['Renewal']}\t{row['Renewal_Count']}\t{money(row['Total New Revenue'])}\t{format_val(row['ROAS'])}\t{format_val(row['ROI'])}\t{format_val(row['L3_ROAS'])}\t{format_val(row['L7_ROAS'])}\t{format_val(row['L14_ROAS'])}\t{format_val(row['L3_ROI'])}\t{format_val(row['L7_ROI'])}\t{format_val(row['L14_ROI'])}\t{row['ROAS_Indicator']}\t{row['ROI_Indicator']}")

def debug_private_key(ga4_config):
    try:
        if isinstance(ga4_config, dict) and 'private_key' in ga4_config:
            private_key = ga4_config['private_key']
            print("Private Key Debug:")
            print(f"  Length: {len(private_key)}")
            print(f"  First 50 chars: {repr(private_key[:50])}")
            print(f"  Last 50 chars: {repr(private_key[-50:])}")
            contains_escaped_newline = '\\n' in private_key
            print(f"  Contains \\n: {contains_escaped_newline}")
            print(f"  Contains actual newlines: {chr(10) in private_key}")
            print(f"  Starts with header: {private_key.startswith('-----BEGIN PRIVATE KEY-----')}")
            print(f"  Ends with footer: {private_key.endswith('-----END PRIVATE KEY-----')}")
            print(f"  Number of lines: {len(private_key.splitlines())}")
            return private_key
    except Exception as e:
        print(f"Error debugging private key: {e}")
    return None

def debug_config(config_file="apps_config.json"):
    try:
        with open(config_file, 'r') as f:
            configs = json.load(f)
        print("Configuration Debug:")
        print("="*50)
        for idx, app_config in enumerate(configs):
            print(f"\nApp {idx+1}:")
            print(f"  App name: {app_config.get('app_name', 'Unnamed')}")
            if 'ga4' in app_config:
                ga4_config = app_config['ga4']
                print(f"  GA4 property_id: {ga4_config.get('property_id', 'Missing')}")
                sa_info = ga4_config.get('service_account_info', {})
                if isinstance(sa_info, dict):
                    print(f"  GA4 config type: Embedded credentials")
                    for key in ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']:
                        present = "Present" if key in sa_info else "MISSING"
                        print(f"    {key}: {present}")
                    debug_private_key(sa_info)
                else:
                    print(f"  GA4 config type: MISSING or not a dict")
            else:
                print("  GA4 config: MISSING")
    except Exception as e:
        print(f"Error reading config: {e}")

if __name__ == "__main__":
    debug_config()
    try:
        with open("apps_config.json") as f:
            configs = json.load(f)
        for idx, app_config in enumerate(configs):
            print(f"\n--- App {idx+1}: {app_config.get('app_name', 'Unnamed')} ---")
            try:
                gads_data = fetch_gads_data(app_config)
                print(f"Google Ads data fetched: {len(gads_data) if gads_data else 0} days")
                ga4_data = fetch_ga4_data(app_config, gads_data)
                print(f"Final data rows: {len(ga4_data) if ga4_data else 0}")
                print_monthly_report(ga4_data)
            except Exception as e:
                print(f"Error processing app {idx+1}: {e}")
    except Exception as e:
        print(f"Error reading apps_config.json: {e}")