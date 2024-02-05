if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    df = data[(data['trip_distance'] != 0) & (data['passenger_count'] != 0)]

    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date

    new_columns = {
        'VendorID': 'vendor_id',
        'RatecodeID': 'ratecode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',
        }
    df.rename(columns=new_columns, inplace=True)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    assert 'vendor_id' in output.columns, 'No vendor_id column!'
    assert (output['passenger_count'] > 0).all(), "passenger_count is not greater than 0"
    assert (output['trip_distance'] > 0).all(), "trip_distance is not greater than 0"
