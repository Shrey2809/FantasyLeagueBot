import csv
import sqlite3
from datetime import datetime
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler



# Function to find the latest CSV file in the specified folder
def find_latest_csv(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not files:
        return None
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
    return os.path.join(folder_path, latest_file)

# Read CSV file and insert data into player_daily_performance table
def insert_data_from_csv(csv_file):
    date = datetime.now().date()
    conn = sqlite3.connect('SI_2024_FANTASY_LEAGUE.db')
    cursor = conn.cursor()
    with open(csv_file, 'r', encoding='utf-8-sig') as file:
        # Connect to the SQLite database

        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Extract relevant data from the CSV row
            player_name = row['Player'].lower()
            team_name = row['Team'].lower()
              # Assuming you want to use the current date
            body_shot_kills = int(row['Body Shot Kills'])
            headshot_kills = int(row['Entry Kills'])
            utility_kills = int(row.get('Utility Kills', 0))  # Assuming 'Utility Kills' may not be present in every row
            opening_kills = int(row.get('Entry Kills', 0))  # Assuming 'Entry Kills' represents opening_kills
            deaths = int(row['Deaths'])
            opening_deaths = int(row['Entry Deaths'])
            round_wins = int(row['Round Wins'])
            plant_points = int(row['Plants'])
            
            # Calculate total points and points per round
            total_points = int(row['Points'])
            points_per_round = total_points / int(row['Rounds'])
            
            # Insert data into player_daily_performance table
            cursor.execute('''
                INSERT INTO player_daily_performance 
                    (player_id, date, body_shot_kills, headshot_kills, utility_kills, opening_kills, deaths, opening_deaths, round_wins, plant_points, total_points, points_per_round, created_at, updated_at) 
                VALUES 
                    ((SELECT player_id FROM players WHERE player_name = ? AND team_name = ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', (player_name, team_name, date, body_shot_kills, headshot_kills, utility_kills, opening_kills, deaths, opening_deaths, round_wins, plant_points, total_points, points_per_round))
            
            
    # Get all the managers' score for the day and insert it into the managers_daily_scores table
    all_manager_ids = cursor.execute('SELECT manager_id FROM managers WHERE in_closed = TRUE').fetchall()
    for manager_id in all_manager_ids:
        manager_id = manager_id[0]
        manager_score = cursor.execute('''
            SELECT SUM(total_points) FROM player_daily_performance 
            WHERE player_id IN (SELECT player_id FROM closed_game_teams WHERE manager_id = ? and is_active = True) AND date = ?
        ''', (manager_id, date)).fetchone()[0]
        cursor.execute('''
            INSERT INTO managers_daily_scores 
                (manager_id, date, closed_game_score, created_at, updated_at) 
            VALUES 
                (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', (manager_id, date, manager_score))
    all_open_manager_ids = cursor.execute('SELECT manager_id FROM managers WHERE in_closed = FALSE').fetchall()
    for manager_id in all_open_manager_ids:
        manager_id = manager_id[0]
        manager_score = cursor.execute('''
            SELECT SUM(total_points) FROM player_daily_performance 
            WHERE player_id IN (SELECT player_id FROM open_game_teams WHERE manager_id = ? and is_active = True) AND date = ?
        ''', (manager_id, date)).fetchone()[0]
        cursor.execute('''
            INSERT INTO managers_daily_scores 
                (manager_id, date, open_game_roster, created_at, updated_at) 
            VALUES 
                (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', (manager_id, date, manager_score))
            
    # Commit changes and close the connection
    conn.commit()
    conn.close()


# Handler class for file system events
class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        # Check if the created file is a CSV file
        if event.src_path.endswith('.csv'):
            print(f"Detected new CSV file: {event.src_path}")
            insert_data_from_csv(event.src_path)
            
# Replace 'your_folder_path' with the actual folder path where your CSV files land
folder_path = 'stats'
event_handler = MyHandler()
observer = Observer()
observer.schedule(event_handler, path=folder_path, recursive=False)
print("Waiting for file...")
observer.start()

try:
    while True:
        pass
except KeyboardInterrupt:
    observer.stop()

# Wait for the observer to finish
observer.join()
print("Ready to accept file...")

