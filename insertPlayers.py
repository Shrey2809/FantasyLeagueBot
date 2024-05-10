# Insert data into EU Stage 1 Table

import sqlite3
import csv

# open EU Players csv file
with open('players.csv', 'r') as file:
    # read the csv file
    data = csv.reader(file)
    # create a connection to the database
    conn = sqlite3.connect('databases/Manchester.db')
    cursor = conn.cursor()
    # loop through the data and insert into the database
    for i, row in enumerate(data):
        if i != 0:
            print(row[0].strip())
            cursor.execute(f'''
                INSERT INTO players
                (player_name, team_name)
                VALUES('{row[0]}', '{row[1]}');''')
    conn.commit()
    conn.close()