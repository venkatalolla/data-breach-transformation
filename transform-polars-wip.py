import polars as pl
import sqlalchemy as db


def create_empty_df():
    empty_df = pl.DataFrame()


def read_csv_to_df(filename):
    raw_data = pl.read_csv(filename, try_parse_dates=True)
    raw_df = raw_data.select(pl.col("*").exclude("Sources"))
    return raw_df

# def get_diff_columns(raw_df):


def write_to_db(cleansed_df):
    pl.DataFrame.write_database(
        cleansed_df, 'cleansed', 'postgresql://postgres@localhost/breach', if_table_exists='append', engine='sqlalchemy')


new_df = read_csv_to_df("df_1.csv")

is_numeric_mask = new_df['Records'].dtype.is_numeric()

print(new_df.with_columns(is_numeric=is_numeric_mask))
