from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error
from dagster import asset, AssetExecutionContext, MetadataValue

@asset()
def randomforest_model(transformed_traing_data):
    transformed_X_train, transformed_y_train = transformed_traing_data
    rfr = RandomForestRegressor(n_estimator=200, max_depth=20)
    rfr.fit(transformed_X_train, transformed_y_train)
    return rfr

# @asset()
# def gradientboost_model(transformed_traing_data):
#     transformed_X_train, transformed_y_train = transformed_traing_data
#     gbr = GradientBoostingRegressor(n_estimators=200, max_depth=4, learning_rate=0.1)
#     gbr.fit(transformed_X_train, transformed_y_train)
#     return gbr

@asset()
def model_performance(transformed_test_data, randomforest_model):
    transformed_X_test, transformed_y_test = transformed_test_data
    score = randomforest_model.score(transformed_X_test, transformed_y_test)
    
    context.add_output_metadata({
        "model": "random forest regressor",
        "params": MetadataValue.__dict__(randomforest_model.get_params()),
        "R squared": MetadataValue.float(score)
    })

    return score