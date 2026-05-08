import requests
import pandas as pd

BASE_URL = "https://data360api.worldbank.org/data360"


def search(query: str, top: int = 50, skip: int = 0):
    payload = {
        "count": True,
        "search": query,
        "top": top,
        "skip": skip,
        "select": "series_description/idno, series_description/name, series_description/database_id",
    }
    response = requests.post(f"{BASE_URL}/searchv2", json=payload)
    response.raise_for_status()
    print(response.status_code)
    print(response.headers)
    return response.json()


if __name__ == "__main__":
    result_json = search("poverty")
    result_df = pd.json_normalize(result_json.get("value", result_json))
    print(result_df)
