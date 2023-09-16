import pickle

import pandas as pd
from darts import TimeSeries
from darts.models import TFTModel
from darts.dataprocessing.transformers import Scaler
from darts.metrics import mape
# import matplotlib.pyplot as plt
from darts import concatenate

from sales_forecaster.utils.params import DATA_PATH, FORECAST_HORIZON, CONTEXT_HORIZON, RND, VAL_START_DATE, MODEL_PATH


def train(df: pd.DataFrame) -> None:

    model_dict = {}
    # Loop through each variant to build and forecast the model
    for variant_id, gdf in df.groupby('variant_id'):
        # Create a TimeSeries object
        series = TimeSeries.from_dataframe(gdf, 'date', 'sales_quantity', freq='D', fillna_value=0)

        # Train-test split (optional)
        train, val = series.split_after(pd.Timestamp(VAL_START_DATE))

        past_covariates = [
            "n_orders",
            "selling_price_mean",
            "discount_mean",
            "selling_price_median",
            "discount_median",
            "selling_price_min",
            "discount_min",
            "selling_price_max",
            "discount_max",
            "campaign_id",
            "impressions",
            "marketing_spend",
            "temperature_2m_mean",
            "shortwave_radiation_sum",
        ]
        covs = []
        for ele in past_covariates:
            ts = TimeSeries.from_dataframe(gdf, 'date', ele, freq='D', fillna_value=0)
            train_ts, val_ts = ts.split_after(pd.Timestamp(VAL_START_DATE))
            covs.append(train_ts)

        covs = concatenate(covs, axis='component')

        # Model selection and training
        model = TFTModel(
            input_chunk_length=CONTEXT_HORIZON,
            output_chunk_length=FORECAST_HORIZON,
            random_state=RND,
            add_relative_index=True,
            add_encoders={
                'cyclic': {'future': ['month']},
                'datetime_attribute': {'future': ['dayofweek']},
                'transformer': Scaler()
            }
        )
        model.fit(train, past_covariates=covs)

        # Forecast next 90 days
        forecast = model.predict(len(val))

        # Evaluate the model on the validation set (optional)
        if val:
            print(f'MAPE Error for variant {variant_id}: {mape(forecast, val)}')

        model_dict[variant_id] = {
            "model": model,
            "forecast": forecast,
            "train": train,
            "val": val,
            "series": series,
        }

    MODEL_PATH.mkdir(exist_ok=True)
    with open(MODEL_PATH / "model_dict.pkl", "wb") as f:
        pickle.dump(model_dict, f)



if __name__ == "__main__":

    df = pd.read_parquet(DATA_PATH / "prep" / "prep_data.parquet")
    train(df)
