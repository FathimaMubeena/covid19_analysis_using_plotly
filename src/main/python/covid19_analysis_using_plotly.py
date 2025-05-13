import os
import polars as pl


def main():
    print("Hello!")
    # Data extraction - Load the data
    parent_dir = os.path.dirname(os.getcwd())
    data_directory = os.path.join(parent_dir, 'resources')

    print(f"Data directory path: {data_directory}")
    file_path = os.path.join(data_directory, 'covid_19_deaths.csv')
    print(f"file path is: {file_path}")

    df_covid = pl.read_csv(file_path)

    # Data Cleaning
    # 1. Rename the columns by removing spaces
    # 2. Apply Casting on DataTypes
    # 2.1 Str to Date
    updated_df = df_covid.with_columns(
        pl.col('Data As Of').str.strptime(pl.Date, '%m/%d/%Y'),
        pl.col('Start Date').str.strptime(pl.Date, '%m/%d/%Y'),
        pl.col('End Date').str.strptime(pl.Date, '%m/%d/%Y'),
        pl.col('End Date').str.to_date('%m/%d/%Y').alias('End Date 2'),
        pl.col('Year').cast(pl.Int64)
    )

    print(updated_df)

    # Data Transformation
    # 1. Filter out unwanted columns
    # 2. Masking the sensitive information from displaying - SSN


    # EDA - Data Visualization



if __name__ == "__main__":
    main()
