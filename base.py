import csv
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('SI_2024_FANTASY_LEAGUE.db')
cursor = conn.cursor()

# Function to insert player names and team names from CSV into players table
def insert_players_from_csv(csv_file):
    print("Inserting Data")
    with open(csv_file, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            player_name = row['Player'].lower()
            team_name = row['Team'].lower()
            role = row['Role'].lower()
            region = row['Region'].lower()
            
            print(f"Player: {player_name}, Team: {team_name}, Role: {role}, Region: {region}")
            
            # Check if player_name and team_name are present in the row
            if player_name and team_name:
                # Insert data into players table
                print(f"Inserting {player_name.lower()} from {team_name.lower()}")
                cursor.execute(f'''
                    INSERT INTO players (player_name, team_name, role, region)
                    VALUES ('{player_name}', '{team_name}', '{role}', '{region}')
                ''')

# Replace 'your_csv_file.csv' with the actual CSV file containing player names and team names
csv_file_path = 'stats//Players.csv'

# Call the function to insert players from the CSV file
insert_players_from_csv(csv_file_path)

# Commit changes and close the connection
conn.commit()
conn.close()
