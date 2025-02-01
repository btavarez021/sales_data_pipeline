import requests

# Your Fivetran API credentials and connector details
FIVETRAN_API_KEY = "EIpnU1lnKHgBR0jJ"
FIVETRAN_API_SECRET = "QlxQuWIXKBkVvWPATKfNF9O40fqTMAif"
FIVETRAN_CONNECTOR_ID = "gasping_vitally"

import base64

encoded_credentials = base64.b64encode(f"{FIVETRAN_API_KEY}:{FIVETRAN_API_SECRET}".encode()).decode()


def trigger_fivetran_sync(api_key, connector_id):
    url = f"https://api.fivetran.com/v1/connectors/{connector_id}/sync"
    payload = {"force": True}
    headers = {
         "Accept": "application/json;version=2",
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.request("POST", url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Sync triggered successfully: {response.status_code}")
        print(f"Response: {response.json()}")  # Print the JSON response for more context
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        if response is not None:
            print(f"Response Content: {response.content.decode()}")
    except requests.exceptions.RequestException as e:
        print(f"Error triggering Fivetran sync: {str(e)}")

trigger_fivetran_sync(FIVETRAN_API_KEY, FIVETRAN_CONNECTOR_ID)