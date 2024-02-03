import sqlite3

# Get the count of players who're in the league:
conn = conn = sqlite3.connect("SI_2024_FANTASY_LEAGUE.db")
cursor = conn.cursor()

data = cursor.execute("SELECT COUNT(*) FROM MANAGERS WHERE IN_CLOSED = FALSE").fetchall()
print(data)
conn.close()
