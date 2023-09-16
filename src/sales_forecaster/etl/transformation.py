import pandas as pd
import numpy as np

import itertools
    

def transform(
    scm_data: pd.DataFrame,
    marketing_campaign_data: pd.DataFrame,
    marketing_spends_data: pd.DataFrame,
    shop_system_data: pd.DataFrame,
    weather_data: pd.DataFrame
) -> pd.DataFrame:
    """Here we basically join all the dataframes together.
    :param scm_data:
    :param marketing_campaign_data:
    :param marketing_spends_data:
    :param shop_system_data:
    :param weather_data:
    :return:
    """
    # before joining let's generate a series of dates starting from the min date in all the dataframes
    # and ending with the max date in all the dataframes
    min_date = min(
        marketing_campaign_data["date"].min(),
        marketing_spends_data["date"].min(),
        shop_system_data["date"].min(),
        weather_data["date"].min()
    )

    max_date = max(
        marketing_campaign_data["date"].max(),
        marketing_spends_data["date"].max(),
        shop_system_data["date"].max(),
        weather_data["date"].max()
    )

    all_dates = pd.date_range(min_date, max_date, freq="1D").to_frame(name="date")

    prods_in_shop_and_scm = set(shop_system_data["variant_id"]).intersection(set(scm_data["variant_id"]))

    shop_system_data = shop_system_data.loc[shop_system_data['variant_id'].isin(prods_in_shop_and_scm)]

    # the main joining key is the date and the variant_id (product id)
    joined_df = all_dates.merge(shop_system_data, on=["date"], how="left")
    joined_df = joined_df.merge(marketing_campaign_data, on=["date", "variant_id"], how="left")
    joined_df = joined_df.merge(marketing_spends_data, on=["date", "variant_id", "campaign_id"], how="left")
    joined_df = joined_df.merge(weather_data, on=["date"], how="left")
    joined_df = joined_df.merge(scm_data, on=["variant_id"], how="left")


    # aggregate the data by date and variant_id
    joined_agg_df = joined_df.groupby(["date", "variant_id"], as_index=False).agg(
        {
            "order_id": 'nunique',
            "price": list,
            "discount": list,
            "sales_quantity": 'sum',
            "campaign_id": "first",
            "impressions": 'first',
            "marketing_spend": 'first',
            "temperature_2m_mean": 'first',
            "shortwave_radiation_sum": 'first',
            "weight": 'first',
            "lens_material": 'first',
            "color": 'first',
            "frame_material": 'first',
            "cost_of_goods_sold": 'first',
            "current_inventory": 'first',
            "lead_time": 'first',
            "rrp": 'first',
        }
    )

    joined_agg_df.rename(columns={"price": "selling_price", "order_id": 'n_orders'}, inplace=True)


    agg_cols = ['selling_price', 'discount']
    funcs = {
        'min': np.min,
        'max': np.max,
        'mean': np.mean,
        'median': np.median,
        'std': np.std,
    }

    for col, func_name in itertools.product(agg_cols, funcs):
        joined_agg_df[f"{col}_{func_name}"] = joined_agg_df[col].apply(lambda x: funcs[func_name](x))

    joined_agg_df.drop(columns=agg_cols, inplace=True)

    return joined_agg_df