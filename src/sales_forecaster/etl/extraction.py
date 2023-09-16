import gspread
import json

import requests

import pandas as pd
from sales_forecaster.utils.params import (
    PATH_TO_GOOGLE_CREDS,
    SHEET_KEY,
    PATH_TO_SHOPSYSTEM_API_DATA,
    schema_name,
    table_name_marketing_campaign_performance,
    table_name_marketing_campaign_spends,
    API_LINK
)
from sales_forecaster.etl.db_utils import get_db_engine


def get_scm_data() -> pd.DataFrame:
    with open(PATH_TO_GOOGLE_CREDS, 'r') as f:
        json_creds = json.load(f)

    # Authenticate
    gc = gspread.service_account_from_dict(json_creds)

    # Open the Google Sheet
    spreadsheet = gc.open_by_key(SHEET_KEY)

    # Choose the first worksheet
    worksheet = spreadsheet.get_worksheet(0)

    # Read data
    all_values = worksheet.get_all_values()

    df = pd.DataFrame(all_values[1:], columns=all_values[0])

    df['variant_id'] = df['variant_id'].astype(int)

    return df


def get_shop_system_data() -> pd.DataFrame:
    df = pd.read_json(PATH_TO_SHOPSYSTEM_API_DATA, lines=True)
    return df


def get_marketing_campaign_data() -> pd.DataFrame:
    db_engine = get_db_engine()

    with db_engine.connect() as conn:
        df = pd.read_sql(
            f"select * from {schema_name}.{table_name_marketing_campaign_performance}",
            conn,
            parse_dates=True
        )
    df['date'] = pd.to_datetime(df['date'])
    return df

def get_marketing_spends_data() -> pd.DataFrame:
    db_engine = get_db_engine()

    with db_engine.connect() as conn:
        df = pd.read_sql(
            f"select * from {schema_name}.{table_name_marketing_campaign_spends}",
            conn
        )
    df['date'] = pd.to_datetime(df['date'])
    return df


def get_api_data() -> pd.DataFrame:
    response = requests.get(API_LINK)

    df = pd.DataFrame.from_dict(response.json()['daily'])
    df['date'] = pd.to_datetime(df['time'])
    df = df.drop(columns=['time'])
    return df


def extract() -> tuple[pd.DataFrame, ...]:
    scm_data = get_scm_data()
    shop_system_data = get_shop_system_data()
    marketing_campaign_data = get_marketing_campaign_data()
    marketing_spends_data = get_marketing_spends_data()
    api_data = get_api_data()

    return scm_data, shop_system_data, marketing_campaign_data, marketing_spends_data, api_data


if __name__ == "__main__":
    print(get_api_data())
