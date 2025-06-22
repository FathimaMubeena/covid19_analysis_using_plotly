import os
import polars as pl
import plotly.express as px
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

# --- Data Loading and Cleaning ---
def load_and_clean_data(file_path):
    """
    Loads the COVID-19 deaths data from a CSV file and performs cleaning.
    """
    try:
        df_covid = pl.read_csv(file_path)

        # Rename columns for consistency
        df_covid = df_covid.rename({
            'Start Date': 'Start_Date',
            'End Date': 'End_Date',
            'Data As Of': 'Data_As_Of'
        })

        # Apply casting on DataTypes
        updated_df = df_covid.with_columns(
            pl.col('Data_As_Of').str.strptime(pl.Date, '%m/%d/%Y'),
            pl.col('Start_Date').str.strptime(pl.Date, '%m/%d/%Y'),
            pl.col('End_Date').str.strptime(pl.Date, '%m/%d/%Y'),
            pl.col('Year').cast(pl.Int64)
        )
        return updated_df
    except Exception as e:
        print(f"Error loading or cleaning data: {e}")
        return pl.DataFrame() # Return empty DataFrame on error

# --- Data Visualization Functions (Modified for Dash) ---
def create_covid_deaths_by_age_figure(df, selected_year):
    """
    Generates a bar chart for COVID deaths by age group for a given year.
    """
    age_groups = ['0-17 years', '18-29 years', '30-39 years', '40-49 years', '50-64 years',
                  '65-74 years', '75-84 years', '85 years and over', 'All Ages']

    filtered_df = df.filter(
        pl.col('State') == 'United States',
        pl.col('Year') == selected_year,
        pl.col('Age Group').is_in(age_groups),
        pl.col('Sex') == 'All Sexes'
    )

    covid_deaths_by_age = (
        filtered_df
        .group_by('Age Group')
        .agg(pl.col('COVID-19 Deaths').sum())
        .sort(by='COVID-19 Deaths', descending=True)
    )

    fig = px.bar(
        covid_deaths_by_age,
        x='Age Group',
        y='COVID-19 Deaths',
        title=f'COVID Deaths {selected_year} by Age Group - United States'
    )
    fig.update_layout(xaxis_title=None)
    return fig

def create_covid_deaths_by_top_5_states_figure(df, selected_year):
    """
    Generates a bar chart for COVID deaths by top 5 states for a given year.
    """
    filtered_df = df.filter(
        pl.col('State') != 'United States',
        pl.col('Year') == selected_year,
        pl.col('Age Group') == 'All Ages',
        pl.col('Sex') == 'All Sexes'
    )

    covid_deaths_by_top_5_states = (
        filtered_df
        .group_by('State')
        .agg(pl.col('COVID-19 Deaths').sum())
        .sort(by='COVID-19 Deaths', descending=True)
        .head(5)
    )

    fig = px.bar(
        covid_deaths_by_top_5_states,
        x='State',
        y='COVID-19 Deaths',
        title=f'COVID Deaths {selected_year} by Top 5 States'
    )
    fig.update_layout(xaxis_title=None)
    return fig

def create_covid_deaths_by_sex_figure(df, selected_year):
    """
    Generates a bar chart for COVID deaths by sex for a given year.
    """
    filtered_df = df.filter(
        pl.col('State') == 'United States',
        pl.col('Year') == selected_year,
        pl.col('Age Group') == 'All Ages',
        pl.col('Sex') != 'All Sexes'
    )

    covid_deaths_by_sex = (
        filtered_df
        .group_by('Sex')
        .agg(pl.col('COVID-19 Deaths').sum())
        .sort(by='COVID-19 Deaths', descending=True)
    )

    fig = px.bar(
        covid_deaths_by_sex,
        x='Sex',
        y='COVID-19 Deaths',
        title=f'COVID Deaths {selected_year} by Sex - United States',
        text_auto='.2s'
    )
    fig.update_layout(xaxis_title=None)
    fig.update_traces(width=0.3, textfont_size=12, textangle=0, textposition='inside')
    return fig


# --- Dash Application Setup ---
app = Dash(__name__)

# Assume the data file is in the same directory as this script for simplicity,
# or adjust the path as necessary.
# For this example, let's assume 'covid_19_deaths.csv' is in a 'resources' folder
# relative to where this script is run. You might need to adjust this path.
# In a real deployment, you'd manage data location more robustly.
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
print(f"Current directory: {current_dir} and parent directory: {parent_dir} ")
# file_path = os.path.join(parent_dir, 'resources', 'covid_19_deaths.csv')

file_path = "/Users/fathimashaik/IdeaProjects/covid19_analysis_using_plotly/src/main/resources/covid_19_deaths.csv"
# if not os.path.exists(os.path.join(current_dir, 'resources')):
#     os.makedirs(os.path.join(current_dir, 'resources'))
# Create a dummy CSV file for demonstration if it doesn't exist
# In a real scenario, you would download the actual data.
if not os.path.exists(file_path):
    print(f"Creating dummy data file at: {file_path}")
    dummy_data = {
        'Start Date': ['01/01/2023', '01/01/2023', '01/01/2023', '01/01/2023', '01/01/2023',
                       '01/01/2022', '01/01/2022', '01/01/2022', '01/01/2022', '01/01/2022'],
        'End Date': ['12/31/2023', '12/31/2023', '12/31/2023', '12/31/2023', '12/31/2023',
                     '12/31/2022', '12/31/2022', '12/31/2022', '12/31/2022', '12/31/2022'],
        'Data As Of': ['09/27/2023', '09/27/2023', '09/27/2023', '09/27/2023', '09/27/2023',
                       '09/27/2023', '09/27/2023', '09/27/2023', '09/27/2023', '09/27/2023'],
        'State': ['United States', 'California', 'New York', 'Texas', 'Florida',
                  'United States', 'California', 'New York', 'Texas', 'Florida'],
        'Age Group': ['All Ages', '0-17 years', '18-29 years', 'All Ages', 'All Ages',
                      'All Ages', 'All Ages', 'All Ages', 'All Ages', 'All Ages'],
        'Sex': ['All Sexes', 'All Sexes', 'All Sexes', 'Male', 'Female',
                'All Sexes', 'All Sexes', 'All Sexes', 'All Sexes', 'All Sexes'],
        'COVID-19 Deaths': [100000, 15000, 12000, 8000, 7000, 80000, 10000, 9000, 6000, 5000],
        'Year': [2023, 2023, 2023, 2023, 2023, 2022, 2022, 2022, 2022, 2022]
    }
    dummy_df = pl.DataFrame(dummy_data)
    dummy_df.write_csv(file_path)
    print("Dummy data file created.")


# Load data globally to avoid reloading on every callback
df = load_and_clean_data(file_path)

print(df)

# Get unique years from the data for dropdown options, ignoring None
available_years = sorted([year for year in df['Year'].unique().to_list() if year is not None])

app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'maxWidth': '1200px', 'margin': 'auto', 'padding': '20px'}, children=[
    html.H1("COVID-19 Data Analysis Dashboard", style={'textAlign': 'center', 'color': '#333'}),

    html.Div([
        html.Label("Select Year:", style={'marginRight': '10px', 'fontWeight': 'bold'}),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': str(year), 'value': year} for year in available_years],
            value=available_years[0] if available_years else None, # Set initial value to the first available year
            clearable=False,
            style={'width': '200px'}
        )
    ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center', 'marginBottom': '30px'}),

    html.Div([
        dcc.Graph(id='age-group-graph', style={'flex': '1', 'minWidth': '48%', 'margin': '1%'}),
        dcc.Graph(id='top-states-graph', style={'flex': '1', 'minWidth': '48%', 'margin': '1%'})
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'center'}),

    html.Div([
        dcc.Graph(id='sex-graph', style={'width': '50%', 'margin': '0 auto'})
    ], style={'display': 'flex', 'justifyContent': 'center', 'marginTop': '20px'})
])

# --- Dash Callbacks ---
@app.callback(
    Output('age-group-graph', 'figure'),
    Output('top-states-graph', 'figure'),
    Output('sex-graph', 'figure'),
    Input('year-dropdown', 'value')
)
def update_graphs(selected_year):
    if selected_year is None:
        # Return empty figures if no year is selected
        return {}, {}, {}

    fig_age = create_covid_deaths_by_age_figure(df, selected_year)
    fig_states = create_covid_deaths_by_top_5_states_figure(df, selected_year)
    fig_sex = create_covid_deaths_by_sex_figure(df, selected_year)
    return fig_age, fig_states, fig_sex

# --- Run the Dash application ---
if __name__ == '__main__':
    # You would typically run app.run_server(debug=True) for development.
    # For this environment, running it without debug and a specific port.
    app.run(host='0.0.0.0', port=8050) # You can change the port if needed