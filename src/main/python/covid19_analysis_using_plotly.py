import os
import polars as pl
import polars.selectors as cs
import random
import plotly.express as px


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
    # 1. Rename the column 'Start Date' to 'Start_Date'
    df_covid = df_covid.rename({'Start Date': 'Start_Date'})
    df_covid = df_covid.rename({'End Date': 'End_Date'})
    df_covid = df_covid.rename({'Data As Of': 'Data_As_Of'})
    # 1.1 Str to Date
    # 2. Apply Casting on DataTypes
    # 2.1 Str to Date
    updated_df = df_covid.with_columns(
        pl.col('Data_As_Of').str.strptime(pl.Date, '%m/%d/%Y'),
        pl.col('Start_Date').str.strptime(pl.Date, '%m/%d/%Y'),
        pl.col('End_Date').str.strptime(pl.Date, '%m/%d/%Y'),
        pl.col('End_Date').str.to_date('%m/%d/%Y').alias('End_Date 2'),
        pl.col('Year').cast(pl.Int64)
    )

    # 3. Remove Duplicates


    print(updated_df)

    # # Data Transformation
    # # 1. Filter out unwanted columns

    # EDA - Data Visualization
    #Defines a list of age group strings that represent various age ranges, including a total category 'All Ages'.
    age_groups = ['0-17 years', '18-29 years', '30-39 years', '40-49 years', '50-64 years', '65-74 years',
                  '75-84 years', '85 years and over', 'All Ages']


    df_age_groups = updated_df.filter(
        pl.col('Month').is_not_null(),
        pl.col('Age Group').is_in(age_groups))

    print(df_age_groups)


    # Show COVID deaths in 2023 in the US by age group?
    covid_deaths_by_age = (
        updated_df
        .filter(
            pl.col('State') == 'United States',
            pl.col('Year') == 2023,
            pl.col('Age Group').is_in(age_groups),
            pl.col('Sex') == 'All Sexes'
        )
        .group_by('Age Group')
        .agg(pl.col('COVID-19 Deaths').sum())
        .sort(by='COVID-19 Deaths', descending=True)
    )

    fig = px.bar(
        covid_deaths_by_age,
        x='Age Group',
        y='COVID-19 Deaths',
        title='COVID Deaths 2023 by Age Group - As of 9/27/23'
    )

    fig.update_layout(xaxis_title=None)
    fig.show()
    #Display the number of COVID deaths in 2023 in the US by the top five states?
    covid_deaths_by_top_5_states = (
        updated_df
        .filter(
            pl.col('State') != 'United States',
            pl.col('Year') == 2023,
            pl.col('Age Group') == 'All Ages',
            pl.col('Sex') == 'All Sexes'
        )
        .group_by('State')
        .agg(pl.col('COVID-19 Deaths').sum())
        .sort(by='COVID-19 Deaths', descending=True)
        .head()
    )

    fig = px.bar(
        covid_deaths_by_top_5_states,
        x='State',
        y='COVID-19 Deaths',
        title='COVID Deaths 2023 by Top 5 States - As of 9/27/23',
    )

    fig.update_layout(xaxis_title=None)
    fig.show()
    #Display COVID deaths in 2023 in the US by sex, with data labels?
    covid_deaths_by_sex = (
        updated_df
        .filter(
            pl.col('State') == 'United States',
            pl.col('Year') == 2023,
            pl.col('Age Group') == 'All Ages',
            pl.col('Sex') != 'All Sexes'
        )
        .group_by('Sex')
        .agg(pl.col('COVID-19 Deaths').sum())
        .sort(by='COVID-19 Deaths', descending=True)
        .head()
    )

    fig = px.bar(
        covid_deaths_by_sex,
        x='Sex',
        y='COVID-19 Deaths',
        title='COVID Deaths 2023 by Sex - As of 9/27/23',
        text_auto='.2s'
    )

    fig.update_layout(xaxis_title=None)
    fig.update_traces(width=0.3, textfont_size=12, textangle=0, textposition='inside')
    fig.show()




if __name__ == "__main__":
    main()
