import pandas as pd

from data_processing import load_data


def test_load_data_returns_non_empty_dataframe():
    file_name = "Best Prices (Bids).csv"
    data_frame = load_data(file_name)
    assert data_frame is not None, "The DataFrame is None."
    assert isinstance(data_frame, pd.DataFrame), "The returned data is not a DataFrame."
    assert data_frame.empty == False, "The DataFrame is empty."