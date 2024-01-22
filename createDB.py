import sqlite3

conn = sqlite3.connect(f'SI_2024_FANTASY_LEAGUE.db')
cursor = conn.cursor()

# Define Manager Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS managers (
        manager_id INTEGER PRIMARY KEY,
        manager_name TEXT,
        discord_user_id TEXT,
        in_closed BOOLEAN DEFAULT FALSE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# Define Player Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY,
        player_name TEXT,
        team_name TEXT,
        role TEXT,
        region TEXT,
        eliminated BOOLEAN DEFAULT FALSE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# Define Closed Game Teams Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS closed_game_teams (
        team_id INTEGER PRIMARY KEY,
        manager_id INTEGER,
        player_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES managers(manager_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
''')

# Define Open Game Roster Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS open_game_roster (
        roster_id INTEGER PRIMARY KEY,
        manager_id INTEGER,
        player_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES managers(manager_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
''')

# Define Player Daily Performance Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_daily_performance (
        performance_id INTEGER PRIMARY KEY,
        player_id INTEGER,
        date DATE DEFAULT (datetime('now', 'localtime')),
        body_shot_kills INTEGER,
        headshot_kills INTEGER,
        utility_kills INTEGER,
        opening_kills INTEGER,
        deaths INTEGER,
        opening_deaths INTEGER,
        round_wins INTEGER,
        plant_points INTEGER,
        total_points INTEGER,
        points_per_round FLOAT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    )
''')

# Define Manager Daily Scores Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS manager_daily_scores (
        score_id INTEGER PRIMARY KEY,
        manager_id INTEGER,
        date DATE,
        closed_game_score INTEGER,
        open_game_score INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (manager_id) REFERENCES managers(manager_id)
    )
''')

# Define Market Status Table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS market_status (
        market_id INTEGER PRIMARY KEY,
        is_open BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS trades (
	    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
	    requester_id INTEGER,
	    requestee_id INTEGER,
	    requester_player_id INTEGER,
        requestee_player_id INTEGER,
	    date DATE DEFAULT (datetime('now', 'localtime')),
	    is_open BOOLEAN DEFAULT (TRUE),
	    is_accepted BOOLEAN DEFAULT (FALSE),
	    FOREIGN KEY (requester_id) REFERENCES managers(manager_id),
	    FOREIGN KEY (requestee_id) REFERENCES managers(manager_id),
	    FOREIGN KEY (requester_player_id) REFERENCES managers(player_id),
        FOREIGN KEY (requestee_player_id) REFERENCES managers(player_id)
	)
''')

# Commit changes and close the connection
conn.commit()
conn.close()
