import discord
from discord.ext import commands
import logging
import aiohttp
import pandas as pd
import random
import sqlite3
import datetime
from tabulate import tabulate
from fantasyCommandParser import *
from processFile import *

"""
Docstring for all the commands with markdown formatting:
Both closed and open league:
    **+myteam**: Get my current active team
    **+myscore**: Get my total score and rank
    **+standings**: Current Standings for my league
    **+find *Name/Team***: Find a specific player or team
Closed league:
    **+openplayers**: Get top 5 open fraggers and supports 
    **+myrequests**: Get my current open requests
    **+request *ID1/Name1* *ID2/Name2***: Request a player swap
Open league:
    **+signup**: Sign up as a open league player
    **+pick *ID/Name***: Pick a player for the open league
    **+swap *ID1/Name1* *ID2/Name2***: Initiate a trade or accept a trade
"""
class fantasyBotBackend(commands.AutoShardedBot):
    # Initialize the bot
    def __init__(self, config):
        super().__init__(command_prefix="+poll", status=discord.Status.online, intents=discord.Intents.all())
        self.config = config
        self.shard_count = self.config["shards"]["count"]
        shard_ids_list = []

        # create list of shard ids
        for i in range(self.config["shards"]["first_shard_id"], self.config["shards"]["last_shard_id"] + 1):
            shard_ids_list.append(i)
        self.shard_ids = tuple(shard_ids_list)

        self.remove_command("help")
        self.messages = []

        self.admin_users = [400713084232138755, 219643727960866816, 127545629361569792]
        
        
        # BR League DB: 1215021748391514192  1215021748391514192
        # EU League DB: 1215021774501187684
        # SI DB: 840243454537891851
        
        self.league_db = {840243454537891851: "databases/Manchester.db"}

        super().__init__(command_prefix="+help", status=discord.Status.online, intents=discord.Intents.all())

        self.market_open = {}
        for key in self.league_db:
            conn = sqlite3.connect(self.league_db[key])
            cursor = conn.cursor()
            query = cursor.execute(f"""SELECT is_open FROM market_status WHERE market_id = 1""")
            data = query.fetchone()
            if data[0] == 1:
                self.market_open[key] = True
            else:
                self.market_open[key] = False
            conn.close()
            
        
        self.allow_commands = True
        
        self.commands_to_disable = ["+signup", "+pick", "+swap", "+request", "+trade"]
        
        # Configure the logger
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(levelname)s] %(message)s',
            filename='fantasyLeague.log',  # Specify the path to your log file
            filemode='a'  # Use 'a' to append to the file, 'w' to overwrite
        )
        self.logger = logging.getLogger(__name__)
    
    # Generate a random hex color 
    def generate_random_color(self):
        pick_from = [0xf7b720]
        color = random.choice(pick_from)
        return color
    
    # Message displayed when bot is started
    async def on_ready(self):
        self.http_session = aiohttp.ClientSession()
        await self.change_presence(activity=discord.Game(name="use +help for more info"))
        self.logger.info("-------------")
        self.logger.info("|Fantasy Bot|")
        self.logger.info("-------------")

    # Waiting for message
    async def on_message(self, message):      
        # ---------------------------------------------------------------------------------------------------------------------------        
        # Check if message is for the bot and ncan't be used during market close
        if any(message.content.startswith(command) for command in self.commands_to_disable) and not self.market_open[840243454537891851]:
            await message.channel.send("Can't use those commands while the market is closed... Try again later")
            return
        
        # League agnostic commands     
        # Get my current active team
        if message.content.startswith("+myteam"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                query = cursor.execute(f"""
                    SELECT p.player_id, p.player_name, p.team_name
                    FROM teams cgt, players p, managers m 
                    WHERE cgt.manager_id = m.manager_id and p.player_id = cgt.player_id and is_active = TRUE AND m.discord_user_id = {userID}
                """)
                data = query.fetchall()                
                if data is None:
                    await message.channel.send(f"You don't have a team yet, use +signup to get started")
                    return

                # Create a DataFrame with the fetched data
                columns = ['ID', 'Name', 'Team']
                df = pd.DataFrame(data, columns=columns)
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                embed = discord.Embed(title=f"{message.author.name}'s team: ", color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
            except Exception as e:
                await message.channel.send(f"Error getting your team, please try again")
                self.logger.error(e)
            finally:
                conn.close()
        
        # Get my total score and rank
        if message.content.startswith("+myscore"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                # Execute the SQL query and fetch the data into a Pandas DataFrame
                closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                closedCheck = closedCheck.fetchone()
                if closedCheck[0] == True:
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
                            m.manager_id,
                            olr.open_rank AS open_league_rank,
                            SUM(mds.closed_game_score) AS open_league_total_score
                        FROM
                            manager_daily_scores mds
                            JOIN managers m ON mds.manager_id = m.manager_id
                            JOIN ClosedLeagueRank olr ON m.manager_id = olr.manager_id
                        WHERE
                            m.in_closed = TRUE and m.discord_user_id = {userID}
                        GROUP BY
                            m.manager_id, olr.open_rank;
                    """)
                    data = query.fetchall()
                else:
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
                            m.manager_id,
                            olr.open_rank AS open_league_rank,
                            SUM(mds.open_game_score) AS open_league_total_score
                        FROM
                            manager_daily_scores mds
                            JOIN managers m ON mds.manager_id = m.manager_id
                            JOIN OpenLeagueRanks olr ON m.manager_id = olr.manager_id
                        WHERE
                            m.in_closed = false and m.discord_user_id = {userID}
                        GROUP BY
                            m.manager_id, olr.open_rank;
                    """)
                    data = query.fetchall()

                # Create a DataFrame with the fetched data
                columns = ['ID', 'Rank', 'Score']
                df = pd.DataFrame(data, columns=columns)
                df = df[['Rank', 'Score']]
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                embed = discord.Embed(title=f"{message.author.name}'s scores: ", color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
            except Exception as e:
                self.logger.error(e)
            finally:
                conn.close()               
        
        # Current Standings for both leagues
        if message.content.startswith("+standings"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try: 
                type = message.content[11:]
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                if type == "open":
                    df = get_open_table(cursor)
                    table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                    embed = discord.Embed(title=f"Open League Standings: ", color=self.generate_random_color())
                    embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                    await message.channel.send(embed=embed)
                elif type == "closed":
                    df = get_closed_table(cursor)
                    table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                    embed = discord.Embed(title=f"Closed League Standings: ", color=self.generate_random_color())
                    embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                    await message.channel.send(embed=embed)
                else:
                    userID = message.author.id
                    closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                    closedCheck = closedCheck.fetchone()
                    if closedCheck[0] == True:
                        df = get_closed_table(cursor)
                        table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                        embed = discord.Embed(title=f"Closed League Standings: ", color=self.generate_random_color())
                        embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                        await message.channel.send(embed=embed)
                    else:
                        df = get_open_table(cursor)
                        table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                        embed = discord.Embed(title=f"Open League Standings: ", color=self.generate_random_color())
                        embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                        await message.channel.send(embed=embed)
            except Exception as e:
                self.logger.error(e)
            finally:
                conn.close()
                
        # Find a specific player or team
        if message.content.startswith("+find"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                player_data = cursor.execute(f"""SELECT player_id FROM players WHERE 
                                            LOWER(player_name) = LOWER('{message.content[6:]}') or LOWER(team_name) = LOWER('{message.content[6:]}')""")
                player_data = player_data.fetchall()
                simple_list = str([item[0] for item in player_data]).replace('[', '(').replace(']', ')')
                queryTest = f"""
                        SELECT * FROM (          
                                SELECT
                                    player_id,
                                    player_name,
                                    team_name,
                                    max_daily_score,
                                    ROW_NUMBER() OVER (ORDER BY max_daily_score DESC) AS rank
                                FROM
                                    (SELECT
                                        p.player_id,
                                        p.player_name,
                                        p.team_name,
                                        SUM(pdp.total_points) AS max_daily_score
                                    FROM
                                        players p
                                        LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                                    WHERE
                                        p.eliminated = FALSE
                                    GROUP BY
                                        p.player_id) AS subquery
                        ) WHERE 
                            player_id in {simple_list};
                        """
                query = cursor.execute(queryTest)
                data = query.fetchall()
                columns = ['ID', 'Name', 'Team', 'Total Score', 'Rank']
                df = pd.DataFrame(data, columns=columns)
                df = df[['Rank', 'ID', 'Name', 'Team', 'Total Score']]
                if len(df) == 0:
                    raise Exception("Player/team not found")
                embed = discord.Embed(title=f"Search results for {message.content[6:].upper()}", color=self.generate_random_color())
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
            except Exception as e:
                await message.channel.send(f"Error finding player or team, please try again: {e}")
                self.logger.error(e)
            finally:
                conn.close()
            
        # Get top 5 open fraggers and supports and send file
        if message.content.startswith("+openplayers"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                query = cursor.execute("""SELECT p.player_id, p.player_name, p.team_name, SUM(pdp.total_points) AS max_daily_score
                                            FROM players p
                                            LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                                            WHERE p.eliminated = FALSE AND
                                                p.player_id NOT IN (
                                                SELECT cgt.player_id
                                                FROM teams cgt WHERE is_active = 1 and manager_id in (SELECT manager_id FROM managers WHERE in_closed = TRUE)
                                            )
                                            GROUP BY p.player_id
                                            ORDER BY max_daily_score DESC;""")
                data = query.fetchall()
                columns = ['Player ID', 'Player Name', 'Team Name', 'Total Score']
                df = pd.DataFrame(data, columns=columns)
                df1 = df[:50]
                df2 = df[50:]
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                with open('openPlayerStats.md', 'w', encoding="utf-8") as f:
                    f.write(table)
                await message.channel.send(file=discord.File('openPlayerStats.md'))
            except Exception as e:
                self.logger.error(e)
            finally:
                conn.close()
           
        # Get all players and their scores and send file             
        if message.content.startswith("+allplayers"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                query = cursor.execute("""SELECT p.player_id, p.player_name, p.team_name, SUM(pdp.total_points) AS max_daily_score
                                            FROM players p
                                            LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                                            WHERE p.eliminated = FALSE
                                            GROUP BY p.player_id
                                            ORDER BY max_daily_score DESC;""")
                data = query.fetchall()
                columns = ['Player ID', 'Player Name', 'Team Name', 'Total Score']
                df = pd.DataFrame(data, columns=columns)
                df1 = df[:50]
                df2 = df[50:]
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                with open('playerStats.md', 'w', encoding="utf-8") as f:
                    f.write(table)
                await message.channel.send(file=discord.File('playerStats.md'))
            except Exception as e:
                self.logger.error(e)
            finally:
                conn.close()           
        
        # Get all scores for a user 
        if message.content.startswith("+dailyscores"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                # Execute the SQL query and fetch the data into a Pandas DataFrame
                closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                closedCheck = closedCheck.fetchone()
                if closedCheck[0] == True:
                    query = cursor.execute(f"""
                    SELECT 
                        closed_game_score,
                        date
                    FROM 
                        manager_daily_scores mds
                        INNER JOIN managers m ON m.manager_id = mds.manager_id and m.discord_user_id = {userID}
                    """)
                    data = query.fetchall()
                else:
                    query = cursor.execute(f"""
                    SELECT 
                        open_game_score,
                        date
                    FROM 
                        manager_daily_scores mds
                        INNER JOIN managers m ON m.manager_id = mds.manager_id and m.discord_user_id = {userID}
                    """)
                    data = query.fetchall()

                # Create a DataFrame with the fetched data
                columns = ['Scores', 'Date']
                df = pd.DataFrame(data, columns=columns)
                df = df[['Date', 'Scores']]
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                embed = discord.Embed(title=f"{message.author.name}'s scores: ", color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
   
            except Exception as e:
                await message.channel.send(f"Error getting scores, please try again")
                self.logger.error(e)
            finally:
                conn.close()
        
        # ---------------------------------------------------------------------------------------------------------------------------   
        # Closed league commands    
        # Get my current open requests - NOT WORKING
        if message.content.startswith("+myrequests") and 4 == 3:
            self.logger.info(f"Message from {message.author}: {message.content}")
            try: 
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                closedCheck = closedCheck.fetchone()
                if closedCheck[0] == False:
                    conn.close() 
                    await message.channel.send(f"You don't need to trade, just swap players using +swap *ID1* *ID2*")
                    return
                
                # Outgoing trades
                query = cursor.execute(f"""SELECT
                                                trades.trade_id,
                                                requester_player.player_name AS requester_player_name,
                                                requestee_player.player_name AS requestee_player_name
                                            FROM trades
                                            JOIN managers AS requester ON trades.requester_id = requester.manager_id
                                            JOIN players AS requester_player ON trades.requester_player_id = requester_player.player_id
                                            JOIN players AS requestee_player ON trades.requestee_player_id = requestee_player.player_id
                                            WHERE trades.is_open = TRUE
                                            AND LOWER(requester.discord_user_id) = LOWER('{userID}'); """)
                data = query.fetchall() 
                
                columns = ['ID', 'My Player', 'Trade For']
                df = pd.DataFrame(data, columns=columns)
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                embed = discord.Embed(title=f"Outgoing trade requests: ", color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await message.channel.send(embed=embed)
            except Exception as e:
                self.logger.error(e)
            finally:
                conn.close()
               
        # Request a player swap - NOT WORKING
        if message.content.startswith("+request") and self.market_open[message.channel.id] == True and self.allow_commands == True and 4 == 3:
            self.logger.info(f"Message from {message.author}: {message.content}")
            try: 
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                closedCheck = closedCheck.fetchone()
                if closedCheck[0] == False:
                    conn.close()
                    await message.channel.send(f"You don't need to request swaps, just swap players using **+swap *ID1/Name* *ID2/Name***")
                    return
                
                request_data = parse_request(message.content)
                
                myPlayerQuery = cursor.execute(f"""SELECT player_id, role FROM players WHERE player_id = '{request_data['MyPlayer']}' or LOWER(player_name) = LOWER('{request_data['MyPlayer']}')""")
                myPlayerData = myPlayerQuery.fetchone()
                myPlayerID = myPlayerData[0]
                myPlayerRole = myPlayerData[1]
                
                # Query to check if my player is actually on my team
                myTeamCheck = cursor.execute(f"""SELECT player_id FROM closed_game_teams WHERE player_id = {myPlayerID} and is_active = TRUE and manager_id = (SELECT manager_id FROM managers WHERE discord_user_id = {userID})""")
                myTeamCheck = myTeamCheck.fetchone()
                if myTeamCheck is None:
                    await message.channel.send(f"Can't request to swap out a player that isn't on your team!")
                    return
                
                requestedPlayerQuery = cursor.execute(f"""SELECT player_id, role FROM players WHERE player_id = '{request_data['TradeFor']}' or LOWER(player_name) = LOWER('{request_data['TradeFor']}')""")
                requestedPlayerData = requestedPlayerQuery.fetchone()
                requestedPlayerID = requestedPlayerData[0]
                requestedPlayerRole = requestedPlayerData[1]

                # Query to check if the requested player is not on a team
                requestedTeamCheck = cursor.execute(f"""SELECT player_id FROM closed_game_teams WHERE player_id = {requestedPlayerID} and is_active = TRUE""")
                requestedTeamCheck = requestedTeamCheck.fetchone()
                if requestedTeamCheck is not None:
                    await message.channel.send(f"Can't request to swap in a player that is already on a team!")
                    return
                
                # if requestedPlayerRole != myPlayerRole:
                #     await message.channel.send(f"Can't request to swap players with differing roles. Please swap for the same roles")
                #     return
                
                # Get the manager id of the user
                query = cursor.execute(f"""SELECT manager_id FROM managers WHERE discord_user_id = '{userID}'""")
                data = query.fetchone()
                manager_id = data[0]
                
                # Insert data into the trades table
                cursor.execute(f"""INSERT INTO trades (requester_id, requester_player_id, requestee_player_id, is_accepted, is_open) VALUES 
                            ({manager_id}, {myPlayerID}, {requestedPlayerID}, FALSE, TRUE)""")
                
                trade_id = cursor.lastrowid
                user = await self.fetch_user(400713084232138755)
                await message.channel.send(f"Swap request submitted")
                await user.send(f'Trade request with ID: {trade_id} for {requestedPlayerID} from {message.author.name}.')
                
                conn.commit()
            except Exception as e:
                self.logger.error(e)
            finally:
                conn.close()
               
        # ---------------------------------------------------------------------------------------------------------------------------   
        # Open league commands
        # Sign up as a open league player +signup
        if message.content.startswith("+signup") and self.market_open[840243454537891851] == True and self.allow_commands == True:
            try:
                self.logger.info(f"Message from {message.author}: {message.content}")
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                closedCheck = closedCheck.fetchone()
                if closedCheck is not None:
                    if closedCheck[0] == True:
                        conn.close()
                        await message.channel.send(f"You're already in the closed league!")
                        return
                
                username = message.author.name
                query = cursor.execute(f"""SELECT manager_id FROM managers WHERE discord_user_id = '{userID}'""")
                data = query.fetchone()
                if data is None:
                    cursor.execute(f"""INSERT INTO managers (manager_name, discord_user_id, in_closed) VALUES ('{username}', '{userID}', FALSE)""")
                    
                    user = await self.fetch_user(userID)
                    
                    query = cursor.execute("""SELECT p.player_id, p.player_name, p.team_name, p.role, SUM(pdp.total_points) AS max_daily_score
                                                FROM players p
                                                LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                                                WHERE p.eliminated = FALSE
                                                GROUP BY p.player_id
                                                ORDER BY max_daily_score DESC;""")
                    data = query.fetchall()
                    columns = ['Player ID', 'Player Name', 'Team Name', 'Role', 'Total Score']
                    df = pd.DataFrame(data, columns=columns)
                    df1 = df[:50]
                    df2 = df[50:]
                    table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                    with open('playerStats.md', 'w', encoding="utf-8") as f:
                        f.write(table)
                    
                    await user.send(f"Welcome to the open league! Use +pick *ID* or +pick *name* to pick players", file=discord.File('playerStats.md'))
                    await message.channel.send(f"Signed up as {username}, go to DMs to pick players, use +pick *ID* or +pick *name*")
                else:
                    await message.channel.send(f"Already signed up as {username}")
                conn.commit()
            except Exception as e:
                self.logger.error(e)
                await message.channel.send(f"Error signing up, please try again after opening your DMs from this server")
            finally:
                conn.close()
        
        # Pick a player for the open league
        if message.content.startswith("+pick") and self.market_open[840243454537891851] == True and self.allow_commands == True:
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                query = cursor.execute(f"""SELECT manager_id, in_closed FROM managers WHERE discord_user_id = '{userID}'""")
                data = query.fetchone()
                manager_id = data[0]
                if data[1] == True:
                    conn.close()
                    await message.channel.send(f"For closed league, your picks are done in a draft!")
                    return
                player_data = message.content[6:]
                query = cursor.execute(f"""SELECT player_id, role FROM players WHERE player_id = '{player_data}' or LOWER(player_name) = LOWER('{player_data}')""")
                data = query.fetchone()
                if data is None:
                    await message.channel.send(f"Invalid player ID/Name, try again")
                    return
                player_id = data[0]
                role = data[1]
                
                queryForTeamCount = cursor.execute(f"""
                        SELECT p.player_id, p.player_name, p.team_name, p.role
                        FROM teams ogr, players p, managers m 
                        WHERE ogr.manager_id = m.manager_id and p.player_id = ogr.player_id and is_active = TRUE AND m.discord_user_id = {userID}
                    """)
                dataForTeamCount = queryForTeamCount.fetchall()
                queryForSamePlayer = cursor.execute(f"""
                        SELECT p.player_id, p.player_name, p.team_name, p.role
                        FROM teams ogr, players p, managers m 
                        WHERE ogr.manager_id = m.manager_id and p.player_id = ogr.player_id and is_active = TRUE AND m.discord_user_id = {userID} AND p.player_id = {player_id}
                    """)
                dataForSamePlayer = queryForSamePlayer.fetchall()
                if len(dataForTeamCount) >= 5:
                    await message.channel.send(f"Already have 5 players, swap a player using +swap **OldPlayerID/OldPlayerName** **NewPlayerID/NewPlayerName**")
                elif len(dataForSamePlayer) > 0:
                    await message.channel.send(f"Can't pick the same player twice")
                else:
                    cursor.execute(f"""INSERT INTO teams (manager_id, player_id, is_active) VALUES ({manager_id}, {player_id}, TRUE)""")
                    await message.channel.send(f"Player picked! Check your team with +myteam")
                    conn.commit()
            except Exception as e:
                await message.channel.send(f"Pick failed. Try again or contact a mod: {e}")
                self.logger.error(e)
            finally:
                conn.close()
        
        # Initiate a trade or accept a trade
        if message.content.startswith("+swap") and self.market_open[840243454537891851] == True and self.allow_commands == True:
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
                closedCheck = closedCheck.fetchone()
                if closedCheck[0] == True:
                    conn.close()
                    await message.channel.send(f"Use the **+request *ID1/Name* *ID2/Name*** command for closed league trades")
                    return
                
                trade = parse_swap(message.content)
                if trade['Type'] == 'request':
                    myplayerid = trade['MyPlayer']
                    requestedplayerid = trade['TradeFor']
                    if myplayerid == requestedplayerid:
                        # Can't trade for the same player
                        await message.channel.send(f"Can't pull a -Laxing +Laxing right away")
                        return
                    
                    # Get the manager id of the user
                    query = cursor.execute(f"""SELECT manager_id FROM managers WHERE discord_user_id = '{userID}'""")
                    data = query.fetchone()
                    manager_id = data[0]
                    
                    # Get the player id of the player being traded
                    query = cursor.execute(f"""SELECT player_id, role FROM players WHERE player_id = '{myplayerid}' or LOWER(player_name) = LOWER('{myplayerid}')""")
                    data = query.fetchone()
                    requester_player_id = data[0]
                    out_player_role = data[1]
                    print(data)
                    
                    # Get the player id of the player being traded for
                    query = cursor.execute(f"""SELECT player_id, player_name, role FROM players WHERE player_id = '{requestedplayerid}' or LOWER(player_name) = LOWER('{requestedplayerid}')""")
                    data = query.fetchone()
                    requestee_player_id = data[0]
                    requestee_player_name = data[1]
                    in_player_role = data[2]
                    print(requestedplayerid, data)
                    
                    # Check if outgoing player is actually on the team
                    myTeamCheck = cursor.execute(f"""SELECT player_id FROM teams WHERE player_id = {requester_player_id} and is_active = TRUE and manager_id = {manager_id}""")
                    myTeamCheck = myTeamCheck.fetchone()
                    if myTeamCheck is None:
                        await message.channel.send(f"Can't trade a player that isn't on your team!")
                        return
                    
                    if out_player_role != in_player_role:
                        await message.channel.send(f"Can't trade a player with differing roles. Please trade for the same roles")
                        return
                    
                    # Check if incoming player isn't already on my team
                    requestedTeamCheck = cursor.execute(f"""SELECT player_id FROM teams WHERE player_id = {requestee_player_id} and manager_id = {manager_id} and is_active = TRUE""")
                    requestedTeamCheck = requestedTeamCheck.fetchone()
                    if requestedTeamCheck is not None:
                        await message.channel.send(f"Can't trade in a player that is already on your team!")
                        return
                    
                    cursor.execute(f"""INSERT INTO teams (manager_id, player_id, is_active) VALUES ({manager_id}, {requestee_player_id}, TRUE)""")
                    cursor.execute(f"""INSERT INTO trades (requester_id, requester_player_id, requestee_player_id, is_accepted, is_open) VALUES 
                                ({manager_id}, {requester_player_id}, {requestee_player_id}, TRUE, FALSE)""")
                    cursor.execute(f"""UPDATE teams SET is_active = False WHERE player_id = {requester_player_id} and manager_id = {manager_id}""")
                    await message.channel.send(f"Swap complete for next playday!")
                else:
                    await message.channel.send(f"Invalid swap command")
                    
                conn.commit()
            except Exception as e:
                await message.channel.send("Swap failed. Try again or contact a mod")
                self.logger.error(e)
            finally:
                conn.close()
        
        # Help command
        if message.content.startswith("+help"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            await message.channel.send(f"""# :flag_gb: Welcome to the Blast R6 Manchester Major Fantasy League! :flag_gb:
                                            ## The commands you can use for the bot are:
                                                    - **+myteam**: View your current active team
                                                    - **+myscore**: View your total score and rank
                                                    - **+standings**: Current Standings for your league
                                                    - **+signup**: Sign up for the open league
                                                    - **+find *Name/Team***: Find a specific player or team (eg **+find Kheyze** or **+find Furia**)
                                                    - **+pick *ID/Name***: Pick your player using their name or ID (refer to the names from the stats sheet) (eg **+pick Kheyze**)
                                                    - **+swap *MyPlayerName/ID* *RequestedPlayerName/ID***: Make a trade or accept a trade (eg **+trade Kheyze Herdz**)
                                                    - **+dailyscores**: Get your daily scores""")

        # ---------------------------------------------------------------------------------------------------------------------------
        # Admin Controls
        # Upload a new file to the server
        if (message.content.startswith("+upload") or message.content.startswith("+d") 
         or (message.channel.id == 1198778760120500284 and message.guild.id == 1042862967072501860)):
            self.logger.info(f"Message from {message.author}: {message.content}")
            if message.attachments:
                tableName = self.league_db[840243454537891851]
                folderName = 'Manchester'
                
                current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M")

                file_name = f'stats//{folderName}//RAW_TOTALS_{current_datetime}.csv'

                for attachment in message.attachments:
                    await attachment.save(file_name)
                    print(f"File '{attachment.filename}' downloaded from {message.author}. Saved as {file_name}.")  
                
                # Process the file
                insert_data_from_csv(file_name, tableName)
                await message.channel.send(f"File processed and uploaded to the database")
            
        # Get the current market status or open/close the market 
        if message.content.startswith("+market"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                if message.content[8:8+len("open")] == "open" and message.author.id in self.admin_users:
                    tableName = self.league_db[840243454537891851]
                    channelId = 840243454537891851

                    channel = self.get_channel(channelId)
                    
                    conn = sqlite3.connect(tableName)
                    cursor = conn.cursor()
                    cursor.execute(f"""UPDATE market_status SET is_open = 1, updated_at = datetime('now')  WHERE market_id = 1""")
                    await channel.send(f"Market is now open!")
                    self.market_open[channelId] = True
                    conn.commit()
                elif message.content[8:8+len("close")] == "close" and message.author.id in self.admin_users:
                    tableName = self.league_db[840243454537891851]
                    channelId = 840243454537891851    

                    channel = self.get_channel(channelId)

                    conn = sqlite3.connect(tableName)
                    cursor = conn.cursor()
                    cursor.execute(f"""UPDATE market_status SET is_open = 0, updated_at = datetime('now') WHERE market_id = 1""")
                
                    await channel.send(f"Market is now closed!")
                    self.market_open[channelId] = False
                    conn.commit()       
                else:
                    await message.channel.send(f"Invalid market command")
            except Exception as e:
                await message.channel.send(f"Error changing market status, please try again (error: {e})")
            finally:
                conn.close()
                        
        # Get all open trades
        if message.content.startswith("+opentrades") and message.author.id in self.admin_users:
            self.logger.info(f"Message from {message.author}: {message.content}")   
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            # Query to get open trades by ID and get the scores of the manager that is requesting the trade
            query = cursor.execute(f"""SELECT
                                            trades.trade_id,
                                            requester.manager_name AS requester_name,
                                            requester_player.player_name AS requester_player_name,
                                            requestee_player.player_name AS requestee_player_name
                                        FROM trades
                                        JOIN managers AS requester ON trades.requester_id = requester.manager_id
                                        JOIN players AS requester_player ON trades.requester_player_id = requester_player.player_id
                                        JOIN players AS requestee_player ON trades.requestee_player_id = requestee_player.player_id
                                        WHERE trades.is_open = TRUE""")
            data = query.fetchall()
            columns = ['ID', 'From', 'Remove', 'Add']
            df = pd.DataFrame(data, columns=columns)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Open trade requests: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
            
            
            df = get_closed_table(cursor)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Closed League Standings: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
            conn.close()
            
        # Execute a swap
        if message.content.startswith("+accept") and message.author.id in self.admin_users:
            tradeID = message.content[8:]
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            query = cursor.execute(f"""SELECT requester_id, requester_player_id, requestee_player_id 
                                       FROM trades WHERE trade_id = {tradeID} and is_open = TRUE""")
            data = cursor.fetchone()
            manager_id = data[0]
            requester_player_id = data[1]
            requested_player_id = data[2]
            
            # Set the trade to be accepted and closed
            cursor.execute(f"""UPDATE trades SET is_accepted = TRUE, is_open = FALSE, date = datetime('now') WHERE trade_id = {tradeID} or requestee_player_id = {requested_player_id}""")
            cursor.execute(f"""UPDATE teams SET is_active = False, updated_at = datetime('now') WHERE player_id = {requester_player_id} and manager_id = {manager_id}""")
            cursor.execute(f"""INSERT into teams (manager_id, player_id) VALUES ({manager_id}, {requested_player_id})""")
            conn.commit()
            conn.close()
            await message.channel.send(f"Swap complete!")
              
        # Team eleminated update
        if message.content.startswith("+eliminate") and message.author.id in self.admin_users:
            teamName = message.content[11:]
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            cursor.execute(f"""UPDATE players SET eliminated = TRUE, updated_at = datetime('now') WHERE LOWER(team_name) = LOWER('{teamName}')""")
            
            # Go through closed team roster and send DMs to those managers who's team lost players
            manager_ids = cursor.execute(f"""SELECT manager_id FROM teams WHERE player_id IN (SELECT player_id FROM players WHERE eliminated = TRUE) AND manager_id IN (SELECT manager_id FROM managers WHERE in_closed = TRUE)""")
            for manager_id in manager_ids:
                mid = manager_id[0]
                
                user = await self.fetch_user(cursor.execute(f"""SELECT discord_user_id FROM managers WHERE manager_id = {mid}""").fetchone()[0])
                
                # Get the players that were eliminated
                players = cursor.execute(f"""SELECT player_id, player_name FROM players WHERE player_id IN (SELECT player_id FROM closed_game_teams WHERE manager_id = {mid} and is_active = TRUE) and eliminated = TRUE""")
                players = players.fetchall()
                columns = ['ID', 'Player Name']
                df = pd.DataFrame(players, columns=columns)
                table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                embed = discord.Embed(title=f"Swap these players out: ", color=self.generate_random_color())
                embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                await user.send(embed=embed)
            conn.commit()
            conn.close()
        
        # Pause the bot for DB Updates
        if message.content.startswith("+pause") and message.author.id in self.admin_users:
            self.logger.info(f"Message from {message.author}: {message.content}")
            self.allow_commands = False
            await message.channel.send(f"Bot paused!")
        
        # Unpause the bot after DB updates    
        if message.content.startswith("+resume") and message.author.id in self.admin_users:
            self.logger.info(f"Message from {message.author}: {message.content}")
            self.allow_commands = True
            await message.channel.send(f"Bot resumed!")
            
            
        # ---------------------------------------------------------------------------------------------------------------------------
        # Hidden commands not supposed to work now
        # Initiate a trade or accept a trade - working now
        if message.content.startswith("+trade") and self.market_open[840243454537891851] == True:
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db[840243454537891851])
            cursor = conn.cursor()
            closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
            closedCheck = closedCheck.fetchone()
            if closedCheck[0] == False:
                conn.close()
                await message.channel.send(f"You don't need to trade, just swap players using **+swap *ID1* *ID2***")
                return
        
            trade = parse_new_trade(message.content)
            if trade['Type'] == 'trade':
                myplayerid = trade['MyPlayer']
                requestedplayerid = trade['TradeFor']
                if myplayerid == requestedplayerid:
                    # Can't trade for the same player
                    await message.channel.send(f"Can't pull a -Laxing +Laxing right away")
                    return
                
                # Get the manager id of the user
                query = cursor.execute(f"""SELECT manager_id FROM managers WHERE discord_user_id = '{userID}'""")
                data = query.fetchone()
                manager_id = data[0]
                
                # Get the player id of the player being traded
                query = cursor.execute(f"""SELECT player_id, role FROM players WHERE player_id = '{myplayerid}' or LOWER(player_name) = LOWER('{myplayerid}')""")
                data = query.fetchone()
                requester_player_id = data[0]
                out_player_role = data[1]
                
                # Get the player id of the player being traded for
                query = cursor.execute(f"""SELECT player_id, player_name, role FROM players WHERE player_id = '{requestedplayerid}' or LOWER(player_name) = LOWER('{requestedplayerid}')""")
                data = query.fetchone()
                requestee_player_id = data[0]
                requestee_player_name = data[1]
                in_player_role = data[2]
                
                
                # Get the manager id of the manager being traded with
                query = cursor.execute(f"""SELECT cgt.manager_id, m.manager_name, m.discord_user_id
                                            FROM teams cgt, managers m
                                            WHERE player_id = '{requestedplayerid}' and is_active = TRUE AND cgt.manager_id = m.manager_id""")
                data = query.fetchone()
                if data is not None:
                    await message.channel.send(f"Player already on a team, swap not possible")
                    return
                else:
                    print("Player not on a team, swap complete")
                    
                    cursor.execute(f"""INSERT INTO teams (manager_id, player_id) VALUES ({manager_id}, {requestee_player_id})""")
                    cursor.execute(f"""INSERT INTO trades (requester_id, requester_player_id, requestee_player_id, is_accepted, is_open) VALUES 
                                ({manager_id}, {requester_player_id}, {requestee_player_id}, TRUE, FALSE)""")
                    cursor.execute(f"""UPDATE teams SET is_active = False WHERE player_id = {requester_player_id} and manager_id = (SELECT manager_id FROM managers WHERE discord_user_id = {userID})""")
                    await message.channel.send(f"Player not on a team, swap complete")
            elif trade['Type'] == 'accept':
                trade_id = trade['TradeID']
                query = cursor.execute(f"""SELECT requester_id, requestee_id, requester_player_id, requestee_player_id 
                                       FROM trades WHERE trade_id = {trade_id} and is_open = TRUE""")
                data = cursor.fetchone()
                
                # Set the trade to be accepted and closed
                cursor.execute(f"""UPDATE trades SET is_accepted = TRUE, is_open = FALSE WHERE trade_id = {trade_id}""")
                
                # tarade initated (requester): date[0]; player: data[2]
                # trade sent to (requestee)  : date[1]; player: data[3]
                # Remove current players from both teams
                cursor.execute(f"""UPDATE teams SET is_active = False 
                                        WHERE player_id = {data[2]} and manager_id = {data[0]}""")
                cursor.execute(f"""UPDATE teams SET is_active = False 
                                        WHERE player_id = {data[3]} and manager_id = {data[1]}""")
                
                # Insert new players into both teams
                cursor.execute(f"""INSERT INTO teams (manager_id, player_id) VALUES ({data[0]}, {data[3]})""")
                cursor.execute(f"""INSERT INTO teams (manager_id, player_id) VALUES ({data[1]}, {data[2]})""")
                await message.channel.send(f"Trade accepted")
                
            conn.commit()
            conn.close()       
            
        # Get my current open trades both sent and recieved - Not supposed to work
        if message.content.startswith("+mytrades") and 4 == 3:
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
            closedCheck = closedCheck.fetchone()
            if closedCheck[0] == False:
                conn.close() 
                await message.channel.send(f"You don't need to trade, just swap players using +swap *ID1* *ID2*")
                return
            
            # Outgoing trades
            query = cursor.execute(f"""SELECT
                                            trades.trade_id,
                                            requestee.manager_name AS requestee_name,
                                            requester_player.player_name AS requester_player_name,
                                            requestee_player.player_name AS requestee_player_name
                                        FROM trades
                                        JOIN managers AS requester ON trades.requester_id = requester.manager_id
                                        JOIN managers AS requestee ON trades.requestee_id = requestee.manager_id
                                        JOIN players AS requester_player ON trades.requester_player_id = requester_player.player_id
                                        JOIN players AS requestee_player ON trades.requestee_player_id = requestee_player.player_id
                                        WHERE trades.is_open = TRUE
                                        AND LOWER(requester.discord_user_id) = LOWER('{userID}'); """)
            data = query.fetchall() 
            
            columns = ['ID', 'To', 'My Player', 'Trade For']
            df = pd.DataFrame(data, columns=columns)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Outgoing trade requests: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
            
            # Incoming trades
            query = cursor.execute(f"""SELECT
                                            trades.trade_id,
                                            requester.manager_name AS requester_name,
                                            requester_player.player_name AS requester_player_name,
                                            requestee_player.player_name AS requestee_player_name
                                        FROM trades
                                        JOIN managers AS requester ON trades.requester_id = requester.manager_id
                                        JOIN managers AS requestee ON trades.requestee_id = requestee.manager_id
                                        JOIN players AS requester_player ON trades.requester_player_id = requester_player.player_id
                                        JOIN players AS requestee_player ON trades.requestee_player_id = requestee_player.player_id
                                        WHERE trades.is_open = TRUE
                                        AND LOWER(requestee.discord_user_id) = LOWER('{userID}')"""); 
            data = query.fetchall()
            columns = ['ID', 'From', 'Trade For', 'My Player']
            df = pd.DataFrame(data, columns=columns)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Incoming trade requests: ", color=self.generate_random_color(), description="**To accept trade, use +trade accept *ID***")
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed) 
            conn.close()

        # Find a specific player or team - OLD
        if message.content.startswith("+fsdas") and 4 == 3:
            self.logger.info(f"Message from {message.author}: {message.content}")
            try:
                userID = message.author.id
                if message.channel.id in [1215021748391514192, 1215021774501187684]:
                    conn = sqlite3.connect(self.league_db[message.channel.id])
                else: 
                    conn = sqlite3.connect(self.league_db[840243454537891851])
                cursor = conn.cursor()
                player_data = cursor.execute(f"""SELECT player_id FROM players WHERE 
                                            LOWER(player_name) = LOWER('{message.content[6:]}') or LOWER(team_name) = LOWER('{message.content[6:]}')""")
                player_data = player_data.fetchall()
                simple_list = str([item[0] for item in player_data]).replace('[', '(').replace(']', ')')
                roles = ['Fragger', 'Support']
                for role in roles:
                    queryTest = f"""
                            SELECT * FROM (          
                                    SELECT
                                        player_id,
                                        player_name,
                                        team_name,
                                        role,
                                        max_daily_score,
                                        ROW_NUMBER() OVER (ORDER BY max_daily_score DESC) AS rank
                                    FROM
                                        (SELECT
                                            p.player_id,
                                            p.player_name,
                                            p.team_name,
                                            p.role,
                                            SUM(pdp.total_points) AS max_daily_score
                                        FROM
                                            players p
                                            LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                                        WHERE
                                            p.eliminated = FALSE AND p.role = '{role}'
                                        GROUP BY
                                            p.player_id) AS subquery
                            ) WHERE 
                                player_id in {simple_list};
                            """
                    query = cursor.execute(queryTest)
                    data = query.fetchall()
                    columns = ['ID', 'Name', 'Team', 'Role', 'Total Score', 'Rank']
                    df = pd.DataFrame(data, columns=columns)
                    df = df[['Rank', 'ID', 'Name', 'Team', 'Total Score']]
                    if len(df) == 0:
                        return
                    embed = discord.Embed(title=f"Search results for {message.content[6:].upper()} for {role}s: ", color=self.generate_random_color())
                    table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
                    embed.add_field(name='\u200b', value=f'```\n{table}\n```')
                    await message.channel.send(embed=embed)
            except Exception as e:
                await message.channel.send(f"Error finding player or team, please try again")
                self.logger.error(e)
            finally:
                conn.close()


    # Start the bot
    def run(self):
        super().run(self.config["discord_token"], reconnect=True)