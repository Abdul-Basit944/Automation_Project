from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from datetime import datetime, timedelta
from make_client import make_client
import json

def fetch_gads_data(app_config):
    client = make_client(app_config["gads"])
    ga_service = client.get_service("GoogleAdsService")

    today = datetime.today()
    day_2 = (today - timedelta(days=2)).strftime('%Y-%m-%d')
    day_1 = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    campaign_prefix = app_config.get("campaign_prefix", "")
    app_name = app_config.get('app_name', 'Unnamed')
    date_range_days = app_config.get('date_range_days', 2)
    day_start = (today - timedelta(days=date_range_days)).strftime('%Y-%m-%d')

    if campaign_prefix:
        query = f"""
            SELECT
                campaign.name,
                segments.date,
                metrics.cost_micros
            FROM campaign
            WHERE campaign.name LIKE '{campaign_prefix}%'
            AND segments.date BETWEEN '{day_start}' AND '{day_1}'
        """
    else:
        query = f"""
            SELECT
                campaign.name,
                segments.date,
                metrics.cost_micros
            FROM campaign
            WHERE segments.date BETWEEN '{day_start}' AND '{day_1}'
        """

    customer_id = app_config["gads"].get("customer_id")
    if not customer_id:
        print(f" CUSTOMER_ID not found for {app_config.get('app_name', 'Unknown') }.")
        return {}

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        daily_spend = {}
        campaigns_found = set()

        for row in response:
            campaigns_found.add(row.campaign.name)
            # Process spend for all apps now
            raw_date = row.segments.date
            try:
                date_obj = datetime.strptime(str(raw_date), "%Y%m%d")
            except ValueError:
                date_obj = datetime.strptime(str(raw_date), "%Y-%m-%d")
            date_str = date_obj.strftime("%Y-%m-%d")
            spend = row.metrics.cost_micros / 1_000_000
            daily_spend[date_str] = daily_spend.get(date_str, 0) + spend

        # Print all spend days in sorted order
        for date in sorted(daily_spend.keys()):
            print(f"{app_name} | {date}: ${daily_spend[date]:.2f}")

        return daily_spend

    except GoogleAdsException as ex:
        print(f"GoogleAdsException for {app_config.get('app_name', 'Unknown')}: {ex}")
        for error in ex.failure.errors:
            print(f"Error: {error.message}")
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"On field: {field_path_element.field_name}")
        return {}

if __name__ == "__main__":
    with open("apps_config.json") as f:
        configs = json.load(f)
    for idx, app_config in enumerate(configs):
        print(f"\n--- App {idx+1}: {app_config.get('app_name', 'Unnamed')} ---")
        fetch_gads_data(app_config)
