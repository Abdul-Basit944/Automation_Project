import json
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def make_client(gads_config, mcc_id=None) -> GoogleAdsClient:
    credentials = {
        "developer_token": gads_config["developer_token"],
        "refresh_token": gads_config["refresh_token"],
        "client_id": gads_config["client_id"],
        "client_secret": gads_config["client_secret"],
        "use_proto_plus": True
    }

    client = GoogleAdsClient.load_from_dict(credentials)
    client.login_customer_id = mcc_id or gads_config.get("mcc_id")
    return client


if __name__ == "__main__":
    try:
        with open("apps_config.json", "r") as file:
            apps_config = json.load(file)

        if not apps_config or not isinstance(apps_config, list):
            raise ValueError("apps_config.json must contain a list of configurations.")


        client = make_client(apps_config[0]["gads"])
        print(" Google Ads Client created successfully")

    except GoogleAdsException as ex:
        print(" GoogleAdsException:")
        for error in ex.failure.errors:
            print(f'   ↳ {error.message}')
            if error.location:
                for field in error.location.field_path_elements:
                    print(f"     ↳ Field: {field.field_name}")
    except Exception as e:
        print(f" Error: {e}")
