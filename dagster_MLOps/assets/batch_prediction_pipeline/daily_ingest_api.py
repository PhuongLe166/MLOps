import requests
import json
import os
from dagster import asset, MetadataValue
import pandas as pd


@asset(compute_kind='API Ingest')
def ingest_daily_data_from_api(context):
    '''
    Get daily data starting from 2022-01-01 to make batch prediction
    API: https://www.energidataservice.dk/guides/api-guides
    '''
    
    base_url = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"

    params = {

    }

    response = requests.get(base_url, params=params)
    data = response.json()['records']
    
    df = pd.DataFrame(data)
    df['HourDK'] = df['HourDK'] = pd.to_datetime(df['HourDK'])

    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

    return df