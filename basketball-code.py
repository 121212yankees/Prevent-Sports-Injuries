import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Runtime high, ~6 minutes
# Database found on https://www.kaggle.com/datasets/wyattowalsh/basketball?resource=download
# Only uses injury data from 2012-2024

# Base URL for scraping injury data
base_url = "https://www.prosportstransactions.com/basketball/Search/SearchResults.php?Player=&Team=&BeginDate=2012-01-01&EndDate=2022-12-31&InjuriesChkBx=yes&Submit=Search&start="

# Function to scrape a single page
def scrape_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'class': 'datatable'})
    rows = table.find_all('tr')

    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        if len(cols) == 5:
            if cols[2] == '':
                data.append([cols[0], cols[1], cols[3], cols[4]])
            else:
                data.append([cols[0], cols[1], cols[2], cols[4]])
        elif len(cols) == 4:
            data.append([cols[0], cols[1], cols[2], cols[3]])

    return data

# Iterate over multiple pages
all_data = []
start = 0
increment = 25

while True:
    page_url = f"{base_url}{start}"
    page_data = scrape_page(page_url)

    if not page_data:
        break

    all_data.extend(page_data)
    start += increment

# Create a DataFrame
columns = ['Date', 'Team', 'Player', 'Notes']
df_injuries = pd.DataFrame(all_data, columns=columns)
df_injuries['Date'] = pd.to_datetime(df_injuries['Date'])

# Clean the 'Player' column
df_injuries['Player'] = df_injuries['Player'].str.replace(r'^\s*•\s*', '', regex=True).str.strip()

# Remove double entries
df_injuries = df_injuries.drop_duplicates()

# Load player names from the provided CSV file
df_players = pd.read_csv('common_player_info.csv')

# Filter players who are under 45 years old
def calculate_age(birthdate, injury_date):
    return injury_date.year - birthdate.year - ((injury_date.month, injury_date.day) < (birthdate.month, birthdate.day))

df_players['birthdate'] = pd.to_datetime(df_players['birthdate'])
under_45_df = df_players[df_players['birthdate'].apply(lambda x: (datetime.today().year - x.year) < 45)]

# Merge injury data with player data
merged_data = pd.merge(df_injuries, under_45_df, left_on='Player', right_on='display_first_last', how='inner')

# Ensure birthdate and Date columns are in datetime format
merged_data['birthdate'] = pd.to_datetime(merged_data['birthdate'])
merged_data['Date'] = pd.to_datetime(merged_data['Date'])

# Calculate age column
merged_data['age'] = merged_data.apply(lambda row: calculate_age(row['birthdate'], row['Date']), axis=1)

# Sort values by Player and Date
merged_data.sort_values(['Player', 'Date'], inplace=True)

# Define function to calculate return date and days out
def calculate_days_out(df):
    df['Return date'] = None
    df['Days out'] = None
    for idx, row in df.iterrows():
        if 'returned to lineup' not in row['Notes']:
            injury_date = row['Date']
            player = row['Player']
            return_rows = df[(df['Player'] == player) & (df['Notes'].str.contains('returned to lineup')) & (df['Date'] > injury_date)]
            if not return_rows.empty:
                return_date = return_rows.iloc[0]['Date']
                days_out = (return_date - injury_date).days
                df.at[idx, 'Return date'] = return_date
                df.at[idx, 'Days out'] = days_out
    return df

merged_data = merged_data.groupby('Player', group_keys=False).apply(calculate_days_out)

# Keep only necessary columns and rename for clarity
merged_data = merged_data[['Player', 'position', 'height', 'weight', 'age', 'Date', 'Notes', 'Return date', 'Days out']]
merged_data.rename(columns={'Notes': 'Injury', 'Date': 'Injury date'}, inplace=True)

# Ensure at most one in every two entries has a value for Days Out
def filter_days_out(df):
    to_drop = []
    previous_idx = None
    for idx, row in df.iterrows():
        if pd.notna(row['Days out']):
            if previous_idx is not None:
                to_drop.append(previous_idx)
                previous_idx = None
            else:
                previous_idx = idx
    df.drop(to_drop, inplace=True)
    return df

merged_data = merged_data.groupby('Player', group_keys=False).apply(filter_days_out)

# Remove entries with blank Days Out
merged_data = merged_data[merged_data['Days out'].notna()]

# Remove duplicates with the same player name, injury, and return date, keeping the uppermost instance
merged_data = merged_data.drop_duplicates(subset=['Player', 'Injury', 'Return date'], keep='first')

# Clean up 'Return date' format
merged_data['Return date'] = pd.to_datetime(merged_data['Return date']).dt.strftime('%Y-%m-%d')

# Capitalize the first letter of all column headers
merged_data.columns = [col.capitalize() for col in merged_data.columns]

# Remove '(Dnp)' from all injuries if present
merged_data['Injury'] = merged_data['Injury'].str.replace(r'\s*\(DNP\)', '', regex=True).str.strip()

# Remove specified injuries
injuries_to_remove = [
    'ingrown', 'flu', 'surgery', 'itis', 'illness', 'cold', 'rest', 'food', 'headache',
    'sinus', 'virus', 'viral', 'infection', 'COVID', 'NBA', 'tooth', 'conditioning', 'hernia', 'DNP'
]

merged_data = merged_data[~merged_data['Injury'].str.contains('|'.join(injuries_to_remove), case=False)]

# Uncomment the following line to sort by Injury alphabetically instead of Player
# merged_data.sort_values('Injury', inplace=True)

# Reorder columns
merged_data = merged_data[['Player', 'Position', 'Injury', 'Injury date', 'Return date', 'Days out', 'Height', 'Weight', 'Age']]

# Save the merged DataFrame to CSV
csv_path = "Merged_NBA_Player_Injuries_with_details.csv"
merged_data.to_csv(csv_path, index=False)
print(f"CSV file saved at {csv_path}")
