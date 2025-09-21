import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import json
from datetime import datetime

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Placement Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --- GOOGLE SHEETS CONNECTION & DATA LOADING ---

# Use st.cache_data to prevent re-running this function on every interaction.
# The data and connection will be cached.
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_data(worksheet_name):
    """Securely connects to Google Sheets and loads data from a specific worksheet."""
    try:
        # Load credentials from Streamlit's secrets
        creds_dict = st.secrets["gcp_creds"]
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)

        # Get the sheet key from secrets
        sheet_key = st.secrets["gcp_sheet_key"]["key"]
        spreadsheet = client.open_by_key(sheet_key)

        # Load data from the specified worksheet
        worksheet = spreadsheet.worksheet(worksheet_name)
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(
            f"Worksheet '{worksheet_name}' not found. Please ensure the scraper has run for this year and the sheet exists.")
        return pd.DataFrame()  # Return empty dataframe on error
    except Exception as e:
        st.error(f"An error occurred while connecting to Google Sheets: {e}")
        return pd.DataFrame()


# --- DATA PROCESSING AND TRANSFORMATION ---

def process_dataframe(df):
    """Cleans and transforms the raw DataFrame."""
    if df.empty:
        return df

    # Convert 'Date Posted' to datetime objects for time-based analysis
    # Use errors='coerce' to turn any invalid date formats into NaT (Not a Time)
    df['Date Posted'] = pd.to_datetime(df['Date Posted'], format='%d/%m/%Y', errors='coerce')

    # Create a numeric CTC column for calculations.
    # This function will parse the JSON, find the max CTC, and convert it to a number.
    def get_max_ctc(json_str):
        if not isinstance(json_str, str) or not json_str.strip():
            return None
        try:
            salaries = json.loads(json_str)
            if not salaries:
                return None
            # Find the maximum CTC among all programmes for a single company
            max_ctc = max([float(s.get('ctc', 0)) for s in salaries])
            return max_ctc
        except (json.JSONDecodeError, TypeError):
            return None

    df['Max_CTC'] = df['Salaries_FTE_JSON'].apply(get_max_ctc)

    return df


# --- SIDEBAR ---
st.sidebar.title("📊 Placement Dashboard")
st.sidebar.markdown("Use the options below to filter the data.")

selected_year = st.sidebar.selectbox(
    "Select Placement Year",
    options=["2025-26", "2024-25"],  # Add more years as needed
    index=0
)

# CORRECTED LOGIC: Determine the worksheet name based on the selected year.
# "2025-26" should correspond to the suffix "_25"
first_year_part = selected_year.split('-')[0]
year_suffix = first_year_part[-2:]
worksheet_main_name = f"scraped_data_{year_suffix}"
worksheet_ppo_name = f"ppo_data_{year_suffix}"

# --- MAIN PAGE LAYOUT ---
st.title(f"Placement Insights for {selected_year}")

# Load and process the main and PPO data
main_df = load_data(worksheet_main_name)
ppo_df = load_data(worksheet_ppo_name)
main_df = process_dataframe(main_df)

if main_df.empty:
    st.warning("No data available for the selected year. Please select a different year or run the scraper.")
else:
    # --- HIGH-LEVEL METRICS ---
    total_companies = main_df['Company Name'].nunique()
    avg_ctc = main_df['Max_CTC'].mean()
    total_ppos = ppo_df['Company Name'].nunique() if not ppo_df.empty else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Companies", f"{total_companies}")
    col2.metric("Overall Average CTC", f"₹{avg_ctc:,.2f} LPA")
    col3.metric("Companies with PPOs", f"{total_ppos}")

    st.markdown("---")

    # --- GRAPHS AND VISUALIZATIONS ---
    st.subheader("Placement Timeline Analysis")

    # Resample data for graphs
    companies_per_week = main_df.set_index('Date Posted').resample('W')['Company Name'].nunique()
    companies_per_month = main_df.set_index('Date Posted').resample('M')['Company Name'].nunique()
    avg_ctc_per_week = main_df.set_index('Date Posted').resample('W')['Max_CTC'].mean()

    # Create columns for graphs
    graph_col1, graph_col2 = st.columns(2)

    with graph_col1:
        fig_weekly = px.line(
            companies_per_week,
            x=companies_per_week.index,
            y='Company Name',
            title='Companies Arrived per Week',
            labels={'Company Name': 'Number of Companies', 'Date Posted': 'Week'}
        )
        fig_weekly.update_traces(mode='lines+markers')
        st.plotly_chart(fig_weekly, use_container_width=True)

    with graph_col2:
        fig_monthly = px.bar(
            companies_per_month,
            x=companies_per_month.index,
            y='Company Name',
            title='Companies Arrived per Month',
            labels={'Company Name': 'Number of Companies', 'Date Posted': 'Month'}
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

    fig_ctc = px.line(
        avg_ctc_per_week,
        x=avg_ctc_per_week.index,
        y='Max_CTC',
        title='Average CTC Offered per Week (LPA)',
        labels={'Max_CTC': 'Average CTC (in Lakhs)', 'Date Posted': 'Week'}
    )
    st.plotly_chart(fig_ctc, use_container_width=True)

    st.markdown("---")

    # --- DETAILED COMPANY DATA ---
    st.subheader("Detailed Company Information")

    # Sort by date to show the most recent companies first
    main_df_sorted = main_df.sort_values(by='Date Posted', ascending=False)

    for index, row in main_df_sorted.iterrows():
        with st.expander(f"**{row['Company Name']}** - Posted on: {row['Date Posted'].strftime('%d %b, %Y')}"):
            exp_col1, exp_col2, exp_col3 = st.columns(3)

            with exp_col1:
                st.markdown(f"**Arrived For:**")
                st.info(f"{row['Arrived For']}")

            with exp_col2:
                st.markdown("**Salary Details (FTE)**")
                try:
                    salaries = json.loads(row['Salaries_FTE_JSON'])
                    if salaries:
                        st.dataframe(pd.DataFrame(salaries))
                    else:
                        st.text("N/A")
                except (json.JSONDecodeError, TypeError):
                    st.text("N/A")

                st.markdown("**Stipend Details (Internship)**")
                try:
                    stipends = json.loads(row['Stipends_Internship_JSON'])
                    if stipends:
                        st.dataframe(pd.DataFrame(stipends))
                    else:
                        st.text("N/A")
                except (json.JSONDecodeError, TypeError):
                    st.text("N/A")

            with exp_col3:
                st.markdown("**Recruitment Rounds**")
                try:
                    rounds = json.loads(row['Rounds_Shortlists_JSON'])
                    if rounds:
                        st.dataframe(pd.DataFrame(rounds))
                    else:
                        st.text("No round data available.")
                except (json.JSONDecodeError, TypeError):
                    st.text("No round data available.")

    # --- PPO LISTING ---
    if not ppo_df.empty:
        st.markdown("---")
        st.subheader("Pre-Placement Offers (PPOs)")
        st.dataframe(ppo_df)

