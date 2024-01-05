import subprocess
import pandas as pd
from sqlalchemy import create_engine


def install_start_postgres():
    """Intsall and start the postgresql server on mac
    """
    # Install PostgreSQL with Homebrew
    install_postgres_command = "brew install postgresql@15"
    subprocess.run(install_postgres_command, shell=True)

    # Start PostgreSQL service
    start_postgres_command = "brew services start postgresql"
    subprocess.run(start_postgres_command, shell=True)

    # Create postgres user
    user_create_command = "/opt/homebrew/bin/createuser -s postgres"
    subprocess.run(user_create_command, shell=True)


def read_csv_to_df(filename):
    """Read a CSV to a data frame

    Args:
        filename (string): CSV file name

    Returns:
        object: Returns the dataframe
    """
    df = pd.read_csv('data_breaches.csv')
    df = df.drop(columns=['Serial', 'Sources'])
    return df


def write_to_db(db, db_table, df):
    """Write the dataframe to the postgres database

    Args:
        db (string): Database name
        db_table (string): Database table name
        df (object): Dataframe to load to the database
    """
    engine = create_engine(f"postgresql://postgres@localhost/{db}")
    df.to_sql(db_table, engine, if_exists='replace', index=False)


def convert_to_int(df, column_names):
    """_summary_

    Args:
        df (object): DataFrame to transform
        column_names (list): List of column names to convert to int

     Returns:
        object: Returns the new dataframe with columns type casted to int
    """
    for each_column in column_names:
        # Set the non number values to `NaN`
        df[each_column] = pd.to_numeric(df[each_column], errors='coerce')
        # Fill the `NaN` with 0 value
        int_df = df.fillna(0).astype({each_column: 'int'})

    return int_df


def split_rows(df, split_map):
    """Split the rows for multiple columns using a split string and
    reindex the dataframe

    Args:
        df (object): Dataframe to transform
        split_map (dict): Dictionary with column name as key and split sting as value

    Returns:
        object: Returns the transformed dataframe
    """

    for column_name, split_string in split_map.items():
        # Split rows with "-" string
        df[column_name] = df[column_name].str.split(split_string)
        split_df = df.explode(column_name, ignore_index=True)

    return split_df
