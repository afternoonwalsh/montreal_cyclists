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
    # Source: https://climate.weather.gc.ca/climate_data/daily_data_e.html?hlyRange=2013-02-13%7C2023-09-10&dlyRange=2013-02-14%7C2023-09-10&mlyRange=%7C&StationID=51157&Prov=QC&urlExtension=_e.html&searchType=stnProv&optLimit=yearRange&StartYear=2023&EndYear=2023&selRowPerPage=25&Line=147&Month=12&Day=10&lstProvince=QC&timeframe=2&Year=2022
    url = 'https://storage.googleapis.com/montreal-cyclists-pmw/en_climate_daily_QC_7025251_2022_P1D.csv'
    response = requests.get(url)

    return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
