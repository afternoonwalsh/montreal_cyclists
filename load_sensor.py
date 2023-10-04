import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    # Source: https://donnees.montreal.ca/dataset/velos-comptage
    url = 'https://storage.googleapis.com/montreal-cyclists-pmw/localisation_des_compteurs_velo.csv'
    response = requests.get(url)
    result = pd.read_csv(io.StringIO(response.text), sep=',')

    return result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
