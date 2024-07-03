import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re

# Two databases used for player data collection
# https://www.retrosheet.org/biofile.htm for player height, weight, DOB, name, etc.
# https://github.com/toddrob99/MLB-StatsAPI/blob/be8210d89e42625f1db22275d36bf3c6fb559b57/statsapi/endpoints.py for player positions
# Runtime anywhere from 15-30 minutes based on selected injury data dates (changeable on line )

# Load the master file containing player positions
master_data = pd.read_csv('master.csv')

# Clean and prepare the master data
master_data['mlb_name'] = master_data['mlb_name'].apply(lambda x: re.sub(r'\(.*?\)', '', x).strip())
master_data['birth_date'] = pd.to_datetime(master_data['birth_date'], errors='coerce')

# Load the biofile data
biofile_data = pd.read_csv('biofile.csv', low_memory=False)
biofile_data['BIRTHDATE'] = pd.to_datetime(biofile_data['BIRTHDATE'], errors='coerce')

# Combine the nickname and last names to match the master data format
biofile_data['NICKNAME_FULLNAME'] = biofile_data['NICKNAME'] + ' ' + biofile_data['LAST']
biofile_data['FIRST_FULLNAME'] = biofile_data['FIRST'] + ' ' + biofile_data['LAST']

# Ensure consistent name formatting
def clean_name(name):
    if isinstance(name, str):
        return re.sub(r'[^a-zA-Z\s]', '', name).strip().lower()
    return ''

biofile_data['CLEAN_NICKNAME_FULLNAME'] = biofile_data['NICKNAME_FULLNAME'].apply(clean_name)
biofile_data['CLEAN_FIRST_FULLNAME'] = biofile_data['FIRST_FULLNAME'].apply(clean_name)
master_data['CLEAN_MLB_NAME'] = master_data['mlb_name'].apply(clean_name)

# Attempt to merge using NICKNAME_FULLNAME
merged_df = pd.merge(biofile_data, master_data, left_on=['CLEAN_NICKNAME_FULLNAME', 'BIRTHDATE'], right_on=['CLEAN_MLB_NAME', 'birth_date'], how='left')

# For entries where NICKNAME_FULLNAME did not match, try FIRST_FULLNAME
missing_positions = merged_df['mlb_pos'].isna()
remaining_biofile_data = biofile_data[missing_positions].copy()

if not remaining_biofile_data.empty:
    additional_merge = pd.merge(remaining_biofile_data, master_data, left_on=['CLEAN_FIRST_FULLNAME', 'BIRTHDATE'], right_on=['CLEAN_MLB_NAME', 'birth_date'], how='left')
    additional_merge = additional_merge[['FIRST_FULLNAME', 'BIRTHDATE', 'HEIGHT', 'WEIGHT', 'mlb_pos']]
    additional_merge = additional_merge.rename(columns={
        'FIRST_FULLNAME': 'Player',
        'BIRTHDATE': 'DOB',
        'HEIGHT': 'Height',
        'WEIGHT': 'Weight',
        'mlb_pos': 'Position'
    })
    additional_merge = additional_merge[additional_merge['Position'].notna()]

    # Append the additional matches to the merged dataframe
    merged_df = pd.concat([merged_df[merged_df['mlb_pos'].notna()], additional_merge])

# Select only the necessary columns from the merged data
merged_df = merged_df[['NICKNAME_FULLNAME', 'BIRTHDATE', 'HEIGHT', 'WEIGHT', 'mlb_pos']]

# Rename columns to match expected output
merged_df = merged_df.rename(columns={
    'NICKNAME_FULLNAME': 'Player',
    'BIRTHDATE': 'DOB',
    'HEIGHT': 'Height',
    'WEIGHT': 'Weight',
    'mlb_pos': 'Position'
})

# Remove entries where the Position column is empty
merged_df = merged_df[merged_df['Position'].notna()]

# Save the updated dataframe to a new CSV file
merged_df.to_csv("player_data_with_positions.csv", index=False)

# Further processing for injury data
def format_player_name(player_name):
    # Remove text within parentheses and the parentheses themselves
    player_name = re.sub(r'\s*\(.*?\)', '', player_name)
    # Remove the part before the slash and the slash itself
    player_name = re.sub(r'.*\/\s*', '', player_name)
    return player_name.strip()

# Cross-reference player details
def get_player_details(player_name, player_df):
    formatted_name = format_player_name(player_name)

    # Standardize player names in DataFrame
    player_df['Cleaned Player'] = player_df['Player'].apply(lambda x: format_player_name(x))

    # Search for the player in the DataFrame
    player_row = player_df[player_df['Cleaned Player'].str.contains(formatted_name, case=False, na=False)]
    if not player_row.empty:
        return player_row.iloc[0].to_dict()
    else:
        return None

# Scrape injury data
def scrape_injury_data(base_url, start_date):
    all_data = []
    start = 0
    increment = 25
    while True:
        page_url = f"{base_url}{start}"
        response = requests.get(page_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'datatable'})

        if table is None:
            break

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
        if not data:
            break
        all_data.extend(data)
        start += increment

    df_injuries = pd.DataFrame(all_data, columns=['Date', 'Team', 'Player', 'Notes'])
    df_injuries['Date'] = pd.to_datetime(df_injuries['Date'])
    df_injuries = df_injuries[df_injuries['Date'] >= start_date]

    df_injuries['Player'] = df_injuries['Player'].str.replace(r'^\s*•\s*', '', regex=True).str.strip()
    df_injuries = df_injuries.drop_duplicates()
    return df_injuries

# Calculate age
def calculate_age(birthdate, injury_date):
    if pd.isna(birthdate):
        return None
    try:
        if isinstance(birthdate, datetime):
            return injury_date.year - birthdate.year - ((injury_date.month, injury_date.day) < (birthdate.month, birthdate.day))
        birthdate = datetime.strptime(birthdate, '%Y-%m-%d')
        return injury_date.year - birthdate.year - ((injury_date.month, injury_date.day) < (birthdate.month, birthdate.day))
    except ValueError:
        return None

# Calculate days out
def calculate_days_out(df):
    df['Return date'] = None
    df['Days out'] = None
    for idx, row in df.iterrows():
        if pd.isna(row['Notes']) or 'returned to lineup' not in row['Notes']:
            injury_date = row['Date']
            player = row['Player']
            return_rows = df[(df['Player'] == player) & (df['Notes'].str.contains('returned to lineup', na=False)) & (df['Date'] > injury_date)]
            if not return_rows.empty:
                return_date = return_rows.iloc[0]['Date']
                days_out = (return_date - injury_date).days
                # Ensure the days out are reasonable
                if 0 < days_out < 365:
                    df.at[idx, 'Return date'] = return_date
                    df.at[idx, 'Days out'] = days_out
    return df

# Ensure that if two or more injuries by the same player are listed on the same return date, keep only the one with the least days out
def filter_least_days_out(df):
    df = df.sort_values('Days out')
    df = df.drop_duplicates(subset=['Player', 'Return date'], keep='first')
    return df

# Main code for scraping injury data and merging with player details
base_url = "https://www.prosportstransactions.com/baseball/Search/SearchResults.php?Player=&Team=&BeginDate=2005-01-01&EndDate=2024-12-31&InjuriesChkBx=yes&submit=Search&start="
start_date = datetime(2005, 1, 1)

df_injuries = scrape_injury_data(base_url, start_date)
print(f"Found {len(df_injuries)} injury records")

# Load the updated player data with positions
player_df = merged_df  # Use the merged data with positions directly

# Cross-reference and get player details
player_details = []
for player_name in df_injuries['Player'].unique():
    details = get_player_details(player_name, player_df)
    if details:
        player_details.append(details)
    else:
        print(f"No details found for player: {player_name}")

if player_details:
    df_player_details = pd.DataFrame(player_details)

    merged_data = pd.merge(df_injuries, df_player_details, on='Player', how='inner')

    merged_data['DOB'] = pd.to_datetime(merged_data['DOB'], errors='coerce')
    merged_data['Injury date'] = pd.to_datetime(merged_data['Date'])
    merged_data['Age'] = merged_data.apply(lambda row: calculate_age(row['DOB'], row['Injury date']), axis=1)
    merged_data.sort_values(['Player', 'Injury date'], inplace=True)

    merged_data = merged_data.groupby('Player', group_keys=False).apply(calculate_days_out)
    merged_data = merged_data[['Player', 'Position', 'Height', 'Weight', 'Age', 'Injury date', 'Notes', 'Return date', 'Days out']]
    merged_data.rename(columns={'Notes': 'Injury', 'Date': 'Injury date'}, inplace=True)

    merged_data = merged_data.groupby('Player', group_keys=False).apply(filter_least_days_out)

    # Remove entries with blank Days Out
    merged_data = merged_data[merged_data['Days out'].notna()]

    # Clean up 'Return date' format
    merged_data['Return date'] = pd.to_datetime(merged_data['Return date']).dt.strftime('%Y-%m-%d')

    # Capitalize the first letter of all column headers
    merged_data.columns = [col.capitalize() for col in merged_data.columns]

    # Remove '(Dnp)', '(DTD)', and '(out indefinitely)' from all injuries if present
    merged_data['Injury'] = merged_data['Injury'].str.replace(r'\s*\(DNP\)', '', regex=True).str.replace(r'\s*\(DTD\)', '', regex=True).str.replace(r'\s*\(out indefinitely\)', '', regex=True).str.strip()

    # Remove specified injuries
    injuries_to_remove = [
        'ingrown', 'flu', 'itis', 'illness', 'cold', 'rest', 'food', 'headache',
        'sinus', 'virus', 'viral', 'infection', 'COVID', 'NBA', 'tooth', 'conditioning', 'hernia', 'kidney stones', 'fatigue', 'eye', 'undisclosed', 'throat'
    ]
    merged_data = merged_data[~merged_data['Injury'].str.contains('|'.join(injuries_to_remove), case=False, na=False)]

    # Reorder columns
    merged_data = merged_data[['Player', 'Position', 'Injury', 'Injury date', 'Return date', 'Days out', 'Height', 'Weight', 'Age']]

    # Save the merged DataFrame to CSV
    csv_path = "Merged_MLB_Player_Injuries_with_details.csv"
    merged_data.to_csv(csv_path, index=False)
    print(f"CSV file saved at {csv_path}")
else:
    print("No player details found.")
