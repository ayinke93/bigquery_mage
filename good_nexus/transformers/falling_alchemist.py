import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    columns_to_convert = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    data[columns_to_convert] = data[columns_to_convert].apply(pd.to_datetime)


    return data



