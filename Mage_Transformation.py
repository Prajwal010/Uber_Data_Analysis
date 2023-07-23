import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])

    Dim_Datetime = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    Dim_Datetime['pick_hour'] = Dim_Datetime['tpep_pickup_datetime'].dt.hour
    Dim_Datetime['pick_day'] = Dim_Datetime['tpep_pickup_datetime'].dt.day
    Dim_Datetime['pick_month'] = Dim_Datetime['tpep_pickup_datetime'].dt.month
    Dim_Datetime['pick_year'] = Dim_Datetime['tpep_pickup_datetime'].dt.year
    Dim_Datetime['pick_weekday'] = Dim_Datetime['tpep_pickup_datetime'].dt.weekday

    Dim_Datetime['drop_hour'] = Dim_Datetime['tpep_dropoff_datetime'].dt.hour
    Dim_Datetime['drop_day'] = Dim_Datetime['tpep_dropoff_datetime'].dt.day
    Dim_Datetime['drop_month'] = Dim_Datetime['tpep_dropoff_datetime'].dt.month
    Dim_Datetime['drop_year'] = Dim_Datetime['tpep_dropoff_datetime'].dt.year
    Dim_Datetime['drop_weekday'] = Dim_Datetime['tpep_dropoff_datetime'].dt.weekday

    Dim_Datetime['datetime_id'] = Dim_Datetime.index
    Dim_Datetime = Dim_Datetime[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
    
    Dim_passenger_count= df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    Dim_passenger_count['passenger_count_id'] = Dim_passenger_count.index
    Dim_passenger_count=Dim_passenger_count[['passenger_count_id','passenger_count']]
    Dim_trip_distance=df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    Dim_trip_distance['trip_distance_id']=Dim_trip_distance.index
    Dim_trip_distance=Dim_trip_distance[['trip_distance_id','trip_distance']]
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    Dim_rate_code = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    Dim_rate_code['rate_code_id'] = Dim_rate_code.index
    Dim_rate_code['rate_code_name'] = Dim_rate_code['RatecodeID'].map(rate_code_type)
    Dim_rate_code = Dim_rate_code[['rate_code_id','RatecodeID','rate_code_name']]


    Dim_pickup_location = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    Dim_pickup_location['pickup_location_id'] = Dim_pickup_location.index
    Dim_pickup_location = Dim_pickup_location[['pickup_location_id','pickup_latitude','pickup_longitude']] 


    Dim_dropoff_location = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    Dim_dropoff_location['dropoff_location_id'] = Dim_dropoff_location.index
    Dim_dropoff_location = Dim_dropoff_location[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]
    
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    Dim_payment_type = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    Dim_payment_type['payment_type_id'] = Dim_payment_type.index
    Dim_payment_type['payment_type_name'] = Dim_payment_type['payment_type'].map(payment_type_name)
    Dim_payment_type = Dim_payment_type[['payment_type_id','payment_type','payment_type_name']]

    Fact_table = df.merge(Dim_passenger_count, on='passenger_count')\
                .merge(Dim_trip_distance, on='trip_distance') \
                .merge(Dim_rate_code, on='RatecodeID') \
                .merge(Dim_pickup_location, on=['pickup_longitude', 'pickup_latitude']) \
                .merge(Dim_dropoff_location, on=['dropoff_longitude', 'dropoff_latitude'])\
                .merge(Dim_Datetime, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
                .merge(Dim_payment_type, on='payment_type') \
                [['VendorID', 'datetime_id', 'passenger_count_id',
				   'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
				   'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
				   'improvement_surcharge', 'total_amount']]

    #print(Fact_table)
    return {"Dim_Datetime":Dim_Datetime.to_dict(orient="dict"),
    "Dim_passenger_count":Dim_passenger_count.to_dict(orient="dict"),
    "Dim_trip_distance":Dim_trip_distance.to_dict(orient="dict"),
    "Dim_rate_code":Dim_rate_code.to_dict(orient="dict"),
    "Dim_pickup_location":Dim_pickup_location.to_dict(orient="dict"),
    "Dim_dropoff_location":Dim_dropoff_location.to_dict(orient="dict"),
    "Dim_payment_type":Dim_payment_type.to_dict(orient="dict"),
    "Fact_table":Fact_table.to_dict(orient="dict")}



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
