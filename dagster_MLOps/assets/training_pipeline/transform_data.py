import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from dagster import (
    asset, 
    multi_asset, 
    AssetOut,
    Output
)

@asset()
def feature_engineered_data(ingest_data_from_api) -> pd.DataFrame:
    df_feature_engineered = df
    df_feature_engineered['HourDK_year'] = df['HourDK'].dt.year.astype(np.int64)
    df_feature_engineered['HourDK_month'] = df['HourDK'].dt.month.astype(np.int64)
    df_feature_engineered['HourDK_day'] = df['HourDK'].dt.day.astype(np.int64)
    df_feature_engineered['HourDK_hour'] = df['HourDK'].dt.hour.astype(np.int64)
    df_feature_engineered['HourDK_weekofyear'] = df['HourDK'].dt.isocalendar().week.astype(np.int64)
    df_feature_engineered['HourDK_dayofweek'] = df['HourDK'].dt.dayofweek.astype(np.int64)
    df_feature_engineered.drop(columns=['HourDK'])

    return df_feature_engineered

@multi_asset(outs={'training_data': AssetOut(), 'test_data': AssetOut()})
def train_test_data(feature_engineered_data):
    # X = df_feature_engineered.drop(columns=['TotalCon'])
    # y = df_feature_engineered.TotalCon

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    yield Output((X_train, y_train), output_name="training_data")
    yield Output((X_test, y_test), output_name="test_data")

@multi_asset(outs={'scaler': AssetOut(), 'encoder': AssetOut(),"transformed_training_data": AssetOut() })
def transformed_traing_data(training_data):
    X_train, y_train = training_data
    
    scaler = StandardScaler()
    encoder = OneHotEncoder()

    numeric_features = ['HourDK_year','HourDK_month','HourDK_day','HourDK_hour','HourDK_weekofyear','HourDK_dayofweek']
    for feature in numeric_features:
        transformed_X_train[feature] = pd.DataFrame(scaler.fit_transform(pd.DataFrame(X_train[feature])), columns=[feature])
    
    categorical_features = ['PriceArea', 'ConsumerType_DE35']
    for feature in categorical_features:
        transformed_X_train[feature] = pd.DataFrame(encoder.fit_transform(pd.DataFrame(X_train[feature])), columns=[feature])

    transformed_X_train = transformed_X_train.toarray()
    transformed_y_train = np.array(y_train)
    
    # return scaler, encoder, (transformed_X_train, transformed_y_train)
    yield Output(scaler, output_name="scaler")
    yield Output(encoder, output_name="encoder")
    yield Output((transformed_X_train, transformed_y_train), output_name="transformed_training_data")

@asset()
def transformed_test_data(test_data, scaler, encoder):
    X_test, y_test = test_data

    numeric_features = ['HourDK_year','HourDK_month','HourDK_day','HourDK_hour','HourDK_weekofyear','HourDK_dayofweek']
    for feature in numeric_features:
        transformed_X_test[feature] = pd.DataFrame(scaler.transform(pd.DataFrame(X_test[feature])), columns=[feature])
    
    categorical_features = ['PriceArea', 'ConsumerType_DE35']
    for feature in categorical_features:
        transformed_X_test[feature] = pd.DataFrame(encoder.transform(pd.DataFrame(X_test[feature])), columns=[feature])

    transformed_X_test = transformed_X_test.toarray()
    transformed_y_test = np.array(y_test)

    return transformed_X_test, transformed_y_test