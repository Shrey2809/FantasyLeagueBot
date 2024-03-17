import re
import pandas as pd

def parse_trade(command):
    # Define the pattern for the trade request command
    request_pattern = r'\+trade\s+request\s+(\d+)\s+(\d+)'
    accept_pattern = r'\+trade\s+accept\s+(\d+)'
    trade_pattern = r'\+trade\s+(\d+)\s+(\d+)'
    pattern_match = re.match(request_pattern, command)
    accept_match = re.match(accept_pattern, command)
    trade_match = re.match(trade_pattern, command)
    if pattern_match:
        # Extract player IDs from the matched groups
        requester_player_id, requestee_player_id = map(int, pattern_match.groups())

        # Return a tuple containing the parsed player IDs
        return {'Type': 'request', 'MyPlayer': requester_player_id, 'TradeFor': requestee_player_id}
    elif accept_match:
        # Extract player IDs from the matched groups
        request_id = int(re.match(accept_pattern, command).group(1))

        # Return a tuple containing the parsed player IDs
        return {'Type': 'accept', 'TradeID': request_id}

    elif trade_match: 
        # Extract player IDs from the matched groups
        requester_player_id, requestee_player_id = map(int, trade_match.groups())

        # Return a tuple containing the parsed player IDs
        return {'Type': 'trade', 'MyPlayer': requester_player_id, 'TradeFor': requestee_player_id}
    else:
        # Return None if the command doesn't match the expected pattern
        return None
    
def parse_new_trade(command):
    # Define the pattern
    pattern = r"\+trade\s+(\w+)\s+(\w+)"

    # Match the pattern
    match = re.match(pattern, command)

    # Check if there's a match
    if match:
        # Access the matched groups
        word1 = match.group(1)
        word2 = match.group(2)
        return {'Type': 'trade', 'MyPlayer': word1, 'TradeFor': word2}
    else:
        print("No match")
    
def parse_swap(command):
    # Define the pattern for the trade request command
    request_pattern = r'\+swap\s+(\w+)\s+(\w+)'
    pattern_match = re.match(request_pattern, command)
    if pattern_match:
        # Extract player IDs from the matched groups
        requester_player_id, requestee_player_id = pattern_match.groups()

        # Return a dictionary containing the parsed player IDs
        return {'Type': 'request', 'MyPlayer': requester_player_id, 'TradeFor': requestee_player_id}
    else:
        # Return None if the command doesn't match the expected pattern
        return None

def parse_request(command):
    # Define the pattern for the trade request command
    request_pattern = r'\+request\s+(\w+)\s+(\w+)'
    pattern_match = re.match(request_pattern, command)
    if pattern_match:
        # Extract player IDs from the matched groups
        requester_player_id, requestee_player_id = pattern_match.groups()

        # Return a dictionary containing the parsed player IDs
        return {'Type': 'request', 'MyPlayer': requester_player_id, 'TradeFor': requestee_player_id}
    else:
        # Return None if the command doesn't match the expected pattern
        return None

def get_open_table(cursor):
    query = cursor.execute(f"""
                WITH OpenLeagueRanks AS (
                        SELECT
                            m.manager_id,
                            RANK() OVER (ORDER BY SUM(mds.open_game_score) DESC) AS open_rank
                        FROM
                            manager_daily_scores mds
                            JOIN managers m ON mds.manager_id = m.manager_id
                        WHERE
                            m.in_closed = false
                        GROUP BY
                            m.manager_id
                    )
                    SELECT
                        m.manager_id, m.manager_name,
                        olr.open_rank AS open_league_rank,
                        SUM(mds.open_game_score) AS open_league_total_score
                    FROM
                        manager_daily_scores mds
                        JOIN managers m ON mds.manager_id = m.manager_id
                        JOIN OpenLeagueRanks olr ON m.manager_id = olr.manager_id
                    WHERE
                        m.in_closed = false
                    GROUP BY
                        m.manager_id, olr.open_rank
                    ORDER BY
                        olr.open_rank ASC;
                """)
    data = query.fetchall()
    columns = ["Manager ID", "Manager Name", "Rank", "Total Score"]
    df = pd.DataFrame(data, columns=columns)
    df = df[['Rank', 'Manager Name', 'Total Score']]
    df = df.head(10)
    return df

def get_closed_table(cursor):
    query = cursor.execute(f"""
                    WITH ClosedLeagueRank AS (
                        SELECT
                            m.manager_id,
                            RANK() OVER (ORDER BY SUM(mds.closed_game_score) DESC) AS open_rank
                        FROM
                            manager_daily_scores mds
                            JOIN managers m ON mds.manager_id = m.manager_id
                        WHERE
                            m.in_closed = TRUE 
                        GROUP BY
                            m.manager_id
                    )
                    SELECT
                        m.manager_id, m.manager_name,
                        olr.open_rank AS open_league_rank,
                        SUM(mds.closed_game_score) AS open_league_total_score
                    FROM
                        manager_daily_scores mds
                        JOIN managers m ON mds.manager_id = m.manager_id
                        JOIN ClosedLeagueRank olr ON m.manager_id = olr.manager_id
                    WHERE
                        m.in_closed = TRUE
                    GROUP BY
                        m.manager_id, olr.open_rank
                    ORDER BY
                        olr.open_rank ASC;
                """)
    data = query.fetchall()
    columns = ["Manager ID", "Manager Name", "Rank", "Total Score"]
    df = pd.DataFrame(data, columns=columns)
    df = df[['Rank', 'Manager Name', 'Total Score']]
    df = df.head(10)
    return df 
    
    
