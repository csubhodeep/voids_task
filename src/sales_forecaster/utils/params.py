import pathlib

# path configs
BASE_PATH = pathlib.Path(__file__).parent.parent.parent.parent

DATA_PATH = BASE_PATH / "data"


# DATA SOURCES

## 1. Shop system API
PATH_TO_SHOPSYSTEM_API_DATA = DATA_PATH / "sales_data_nexasun.json"

## 2 & 3. Marketing campaign performance & spend

### connnection details for postgres database:
schema_name = "google_ads"
table_name_marketing_campaign_spends = "campaign_spend"
table_name_marketing_campaign_performance = "campaign_performance"
# ideally these should be passed via the runtime environment
user = "jobs_ro"
password = "ilovecaseslol"
host = "voids-jobs.c2wwnfcaisej.eu-central-1.rds.amazonaws.com"
database = "postgres"
port = 5432

connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


## 4. SCM data
SHEET_KEY = "1rU5pdSMJ3UAIwXHzuxBgwYvgn-o9nkosw7TGhfg_Bhw"
GSHEET_LINK = f"https://docs.google.com/spreadsheets/d/{SHEET_KEY}"

### google service account credentials
PATH_TO_GOOGLE_CREDS = BASE_PATH / "creds/jobs-398713-d8caacaf8ab0.json"

## 5. REST API - Hourly Weather data API URL for the last year
API_LINK = "https://archive-api.open-meteo.com/v1/archive?latitude=53.5507&longitude=9.993&start_date=2020-10-01&end_date=2022-09-30&daily=temperature_2m_mean,shortwave_radiation_sum&timezone=Europe%2FBerlin"




