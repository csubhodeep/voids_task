
from sales_forecaster.etl.extraction import extract
from sales_forecaster.etl.transformation import transform
from sales_forecaster.etl.loading import load

def run_pipeline():
    """Run the ETL pipeline."""
    scm_data, shop_system_data, marketing_campaign_data, marketing_spends_data, api_data = extract()
    df = transform(scm_data, marketing_campaign_data, marketing_spends_data, shop_system_data, api_data)
    load(df)


if __name__ == "__main__":
    run_pipeline()