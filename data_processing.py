import datetime

import pandas as pd
import os
import pytest
import seaborn as sns
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO, filename="data_processing.log",
                    format="%(asctime)s:%(levelname)s:%(message)s"
                    )

def load_data(filename) -> pd.DataFrame:
    """
    Load data from a file, must contain file extension
    EX: file.csv
    :param filename:
    :return:
    """
    logging.info(f"Loading data from {filename} at {datetime.datetime.now()}")
    file_type = filename.split('.')[-1]
    data_path = os.path.join(os.path.dirname(__file__), f'./{file_type}/{filename}')
    print(data_path)
    data = pd.read_csv(data_path)
    print(data.head())
    print(data.info())
    logging.info(f"Data finished loading from {filename} at {datetime.datetime.now()}")
    return data


def create_plots(array: list, array_tickers: list):
    """
    Create plots for the data, based on the processed tables located in ./processed

    Example:
        array = ["temporary_df_hold_bids.csv", "temporary_df_hold_orders.csv"]

        array_tickers = ['H2O', 'LST', "O", "FEO", "FE", "COF", "NS", "PT", "OVE"]
    :param array:
    :param array_tickers:
    :return:
    """
    try:
        for df_name in array:

            if os.path.exists(f"./processed"):
                pass
            else:
                os.mkdir(f"./processed")
            for material_ticker_filter in array_tickers:
                logging.info(f"Processing {df_name} at {datetime.datetime.now()} for ticker {material_ticker_filter}")
                df: pd.DataFrame = load_data(f"{df_name}")
                df.describe()
                print(f"Printing {material_ticker_filter} data")
                df = df[df['MaterialTicker'].str.fullmatch(material_ticker_filter) == True]
                df['Total Cost'] = df['ItemCount'] * df['ItemCost']
                df['Date'] = pd.to_datetime(df['collection_timestamp']).dt.date

                df['Suspected duplicate'] = df.duplicated(
                    subset=['MaterialTicker', 'ExchangeCode', 'ItemCost', 'ItemCount', 'CompanyName', 'Date'], keep="first")
                df.to_csv(f'processed/{material_ticker_filter}-{df_name}-with-suspected-duplicates.csv')
                print(df)
                df.drop(df[df['Suspected duplicate'] == True].index, inplace=True)
                df.drop(df[df['ItemCost'] < 0].index, inplace=True)
                df.drop(df[df['ItemCount'] < 0].index, inplace=True)

                grouped :pd.Series= df.groupby(['Date', 'ExchangeCode', 'MaterialTicker'])['ItemCount'].sum()
                grouped :pd.DataFrame= grouped.to_frame()
                grouped.to_csv(f'processed/{material_ticker_filter}-{df_name}-simplified_grouped.csv')
                df['Total Available'] = grouped['ItemCount'].sum()
                logging.info(f"Grouped data for {material_ticker_filter} at {datetime.datetime.now()}")
                logging.info(f"Plotting for {material_ticker_filter} at {datetime.datetime.now()}")



                plt.clf()
                # Create a new figure instance for the next plot
                plt.figure(figsize=(20, 10), dpi=120)

                # Reapply the plot settings for the new figure
                sns.lineplot(x='Date',
                             y='ItemCost',
                             data=df,
                             hue='ExchangeCode',
                             style='ExchangeCode',
                             markers=True,
                             dashes=False,
                             sizes=(1, 5),
                             palette='viridis')
                plt.title(f"Product analysis {material_ticker_filter}", fontsize=20, color='gray', fontweight='bold')
                plt.xticks(rotation=90)  # Rotate x-axis labels again if needed
                print("Applying annotations")
                plt.annotate(
                    f"Source: {str(df_name)}",
                    xy=(0.9, 1.11),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )

                plt.annotate(
                    f"Mean: {round(df['ItemCost'].mean(), 2)}",
                    xy=(0.9, 1.02),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Min: {round(df['ItemCost'].min(), 2)}",
                    xy=(0.9, 1.08),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Max: {round(df['ItemCost'].max(), 2)}",
                    xy=(0.9, 1.05),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )

                print("Saving plot with material ticker filter")
                plt.savefig(f'processed/{material_ticker_filter}-{df_name}.png')
                print("Showing plot")
                # plt.show()

                plt.clf()

                plt.figure(figsize=(20, 10), dpi=120)
                print("Dataframe: ")
                print(df)
                print("Grouped: ")
                print(grouped)
                # Reapply the plot settings for the new figure
                sns.lineplot(x='Date',
                             y='ItemCount',
                             data=grouped,
                             hue='ExchangeCode',
                             style='ExchangeCode',
                             markers=True,
                             dashes=False,
                             sizes=(1, 30))

                plt.title(f"Item Availability Daily for {material_ticker_filter}, split by Market Exchange", fontsize=20, color='gray',
                          fontweight='bold')
                plt.xticks(rotation=90)

                plt.annotate(
                    f"Source: {str(df_name)}",
                    xy=(0.9, 1.11),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )

                plt.annotate(
                    f"Mean: {round(df['ItemCost'].mean(), 2)}",
                    xy=(0.9, 1.02),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Min: {round(df['ItemCost'].min(), 2)}",
                    xy=(0.9, 1.08),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                plt.annotate(
                    f"Max: {round(df['ItemCost'].max(), 2)}",
                    xy=(0.9, 1.05),
                    xycoords='axes fraction',
                    fontsize=12,
                    color='black',
                    fontweight='bold'
                )
                # Rotate x-axis labels again if needed
                print("Saving plot with material ticker filter")
                plt.savefig(f'processed/{material_ticker_filter}-{df_name}.png')
                print("Showing plot")
                logging.info(f"Finished plotting for {material_ticker_filter} at {datetime.datetime.now()}")
                # plt.show()
            del df
            plt.clf()

        return True
    except Exception as e:
        logging.error(f"Error in plotting: {e}")
        return False

def cleanup_processed_files():
    """
    Cleanup processed files
    :return:
    """
    try:
        for file in os.listdir('./processed'):
            os.remove(f'./processed/{file}')
        logging.info(f"Cleaned up processed files at {datetime.datetime.now()}")
        return True
    except Exception as e:
        logging.error(f"Error in cleanup: {e}")
        return False



