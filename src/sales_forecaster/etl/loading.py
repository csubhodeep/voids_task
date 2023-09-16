import pandas as pd
from sales_forecaster.utils.params import DATA_PATH

def load(df: pd.DataFrame):
    prep_data_path = DATA_PATH / "prep"
    prep_data_path.mkdir(exist_ok=True)

    df.to_parquet(prep_data_path / "prep_data.parquet", index=False)
