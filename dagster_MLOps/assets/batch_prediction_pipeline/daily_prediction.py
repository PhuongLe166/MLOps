from dagster import asset
@asset()
def latest_data_prediction(randomforest_model, scaler, encoder):
    pass