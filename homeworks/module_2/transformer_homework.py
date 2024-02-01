if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data.columns = data.columns.str.replace(' ', '_').str.lower()

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    vendorid = 1
    assert vendorid in output['vendorid'].values, 'vendor_id not in the table'
    assert (output['passenger_count'] > 0).all(), "Some rows have passenger_count = 0."
    assert (output['trip_distance'] > 0).all(), "Some rows have trip_distance = 0."

