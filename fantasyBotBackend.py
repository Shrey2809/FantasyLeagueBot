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
        self.league_db = "SI_2024_FANTASY_LEAGUE.db"

        super().__init__(command_prefix="+help", status=discord.Status.online, intents=discord.Intents.all())

        
        
        conn = sqlite3.connect(self.league_db)
        cursor = conn.cursor()
        query = cursor.execute(f"""SELECT is_open FROM market_status WHERE market_id = 1""")
        data = query.fetchone()
        if data[0] == 1:
            self.market_open = True
        else:
            self.market_open = False
            
        conn.close()
        
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
        color = random.randint(0, 0xFFFFFF)  # 0x000000 to 0xFFFFFF (0 to 16777215 in decimal)
        return color
    
    # Message displayed when bot is started
    async def on_ready(self):
        self.http_session = aiohttp.ClientSession()
        self.logger.info("-------------")
        self.logger.info("|Fantasy Bot|")
        self.logger.info("-------------")

    # Waiting for message
    async def on_message(self, message):
        # ---------------------------------------------------------------------------------------------------------------------------
        # League agnostic commands     
        # Get my current active team
        if message.content.startswith("+myteam"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            # Execute the SQL query and fetch the data into a Pandas DataFrame
            closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
            closedCheck = closedCheck.fetchone()
            if closedCheck[0] == True:
                query = cursor.execute(f"""
                    SELECT p.player_id, p.player_name, p.team_name, p.region, p.role
                    FROM closed_game_teams cgt, players p, managers m 
                    WHERE cgt.manager_id = m.manager_id and p.player_id = cgt.player_id and is_active = TRUE AND m.discord_user_id = {userID}
                """)
                data = query.fetchall()
            else:
                query = cursor.execute(f"""
                    SELECT p.player_id, p.player_name, p.team_name, p.region, p.role
                    FROM open_game_roster ogr, players p, managers m 
                    WHERE ogr.manager_id = m.manager_id and p.player_id = ogr.player_id and is_active = TRUE AND m.discord_user_id = {userID}
                """)
                data = query.fetchall()

            # Create a DataFrame with the fetched data
            columns = ['ID', 'Name', 'Team', 'Region', 'Role']
            df = pd.DataFrame(data, columns=columns)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"{message.author.name}'s team: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
            conn.close()
        
        # Get my total score and rank
        if message.content.startswith("+myscore"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
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
                
            conn.close()
        
        # Current Standings for both leagues
        if message.content.startswith("+standings"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            type = message.content[11:]
            conn = sqlite3.connect(self.league_db)
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
            conn.close()
                
        # Find a specific player or team
        if message.content.startswith("+find"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
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
                            p.player_id,
                            p.player_name,
                            p.team_name,
                            p.role,
                            SUM(pdp.total_points) AS max_daily_score,
                            RANK() OVER (ORDER BY SUM(pdp.total_points) DESC) AS player_rank
                        FROM
                            players p
                            LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                        WHERE
                            p.eliminated = FALSE and p.role = '{role}'
                        GROUP BY
                            p.player_id
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
            conn.close()
            
        # Get top 5 open fraggers and supports and send embed
        if message.content.startswith("+openplayers"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            query = cursor.execute("""SELECT p.player_id, p.player_name, p.team_name, p.role, SUM(pdp.total_points) AS max_daily_score
                                        FROM players p
                                        LEFT JOIN player_daily_performance pdp ON p.player_id = pdp.player_id
                                        WHERE p.eliminated = FALSE AND
                                            p.player_id NOT IN (
                                            SELECT cgt.player_id
                                            FROM closed_game_teams cgt
                                        )
                                        GROUP BY p.player_id
                                        ORDER BY max_daily_score DESC;""")
            data = query.fetchall()
            columns = ['Player ID', 'Player Name', 'Team Name', 'Role', 'Total Score']
            df = pd.DataFrame(data, columns=columns)
            support_df = df[df['Role'] == 'Support']
            support_df = support_df.head(5)
            support_df = support_df[['Player ID', 'Player Name', 'Team Name', 'Total Score']]
            table = tabulate(support_df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Open Supports: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
            
            fragger_df = df[df['Role'] == 'Fragger']
            fragger_df = fragger_df.head(5)
            fragger_df = fragger_df[['Player ID', 'Player Name', 'Team Name', 'Total Score']]
            table = tabulate(fragger_df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Open Fraggers: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)

            conn.close()
        
        # ---------------------------------------------------------------------------------------------------------------------------   
        # Closed league commands    
        # Get my current open requests
        if message.content.startswith("+myrequests"):
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
            conn.close()
               
        # Request a player swap
        if message.content.startswith("+request") and self.market_open == True:
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
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
            
            if requestedPlayerRole != myPlayerRole:
                await message.channel.send(f"Can't request to swap players with differing roles. Please swap for the same roles")
                return
            
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
            conn.close()
               
        # ---------------------------------------------------------------------------------------------------------------------------   
        # Open league commands
        # Sign up as a open league player +signup
        if message.content.startswith("+signup"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
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
                await message.channel.send(f"Signed up as {username}, to pick players, use +pick *ID* or +pick *name*")
            else:
                await message.channel.send(f"Already signed up as {username}")
            conn.commit()
            conn.close()
        
        # Pick a player for the open league
        if message.content.startswith("+pick") and self.market_open == True:
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
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
            player_id = data[0]
            role = data[1]
            
            query = cursor.execute(f"""
                    SELECT p.player_id, p.player_name, p.team_name, p.region, p.role
                    FROM open_game_roster ogr, players p, managers m 
                    WHERE ogr.manager_id = m.manager_id and p.player_id = ogr.player_id and is_active = TRUE AND m.discord_user_id = {userID} AND role = '{role}'
                """)
            data = query.fetchall()
            query2 = cursor.execute(f"""
                    SELECT p.player_id, p.player_name, p.team_name, p.region, p.role
                    FROM open_game_roster ogr, players p, managers m 
                    WHERE ogr.manager_id = m.manager_id and p.player_id = ogr.player_id and is_active = TRUE AND m.discord_user_id = {userID}
                """)
            data2 = query2.fetchall()
            if role.lower() == 'support' and len(data) >= 2:
                await message.channel.send(f"Already have 2 supports, pick a fragger")
            elif role.lower() == 'fragger' and len(data) >= 3:
                await message.channel.send(f"Already have 3 fraggers, pick a support")
            elif len(data2) >= 5:
                await message.channel.send(f"Already have 5 players, swap a player using +swap **OldPlayerID** **NewPlayerID**")
            else:
                cursor.execute(f"""INSERT INTO open_game_roster (manager_id, player_id, is_active) VALUES ({manager_id}, {player_id}, TRUE)""")
                await message.channel.send(f"Player picked! Check your team with +myteam")
                conn.commit()
            conn.close()
        
        # Initiate a trade or accept a trade
        if message.content.startswith("+swap") and self.market_open == True:
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
            closedCheck = closedCheck.fetchone()
            if closedCheck[0] == True:
                conn.close()
                await message.channel.send(f"Use the **+trade request *ID1/Name* *ID2/Name*** command for closed league trades")
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
                
                # Get the player id of the player being traded for
                query = cursor.execute(f"""SELECT player_id, player_name, role FROM players WHERE player_id = '{requestedplayerid}' or LOWER(player_name) = LOWER('{myplayerid}')""")
                data = query.fetchone()
                requestee_player_id = data[0]
                requestee_player_name = data[1]
                in_player_role = data[2]
                
                if out_player_role != in_player_role:
                    await message.channel.send(f"Can't trade a player with differing roles. Please trade for the same roles")
                    return
                
                cursor.execute(f"""INSERT INTO open_game_roster (manager_id, player_id) VALUES ({manager_id}, {requestee_player_id})""")
                cursor.execute(f"""INSERT INTO trades (requester_id, requester_player_id, requestee_player_id, is_accepted, is_open) VALUES 
                            ({manager_id}, {requester_player_id}, {requestee_player_id}, TRUE, FALSE)""")
                cursor.execute(f"""UPDATE open_game_roster SET is_active = False WHERE player_id = {requester_player_id} and manager_id = {manager_id}""")
                await message.channel.send(f"Swap complete for next playday!")
            else:
                await message.channel.send(f"Invalid swap command")
                
            conn.commit()
            conn.close()
        
        # ---------------------------------------------------------------------------------------------------------------------------
        # Admin Controls
        # Upload a new file to the server
        if (message.content.startswith("+upload") or message.content.startswith("+d") 
         or (message.channel.id == 1198778760120500284 and message.guild.id == 1042862967072501860)):
            self.logger.info(f"Message from {message.author}: {message.content}")
            if message.attachments:
                current_datetime = datetime.datetime.datetime('now').strftime("%Y%m%d%H%M")
                file_name = f'stats//RAW_TOTALS_{current_datetime}.csv'

                for attachment in message.attachments:
                    await attachment.save(file_name)
                    print(f"File '{attachment.filename}' downloaded from {message.author}. Saved as {file_name}.")  
            
        # Get the current market status or open/close the market 
        if message.content.startswith("+market"):
            self.logger.info(f"Message from {message.author}: {message.content}")
            if len(message.content) == 7:
                conn = sqlite3.connect(self.league_db)
                cursor = conn.cursor()
                query = cursor.execute(f"""SELECT is_open FROM market_status WHERE market_id = 1""")
                data = query.fetchone()
                if data[0] == 0:
                    await message.channel.send(f"Market is closed, please wait for the end of the playday to trade or swap players!")
                    conn.close()
                else:
                    await message.channel.send(f"Market is open, you can trade or swap players!")
                    conn.close()
            else:
                if message.content[8:] == "open" and message.author.id in self.admin_users:
                    conn = sqlite3.connect(self.league_db)
                    cursor = conn.cursor()
                    cursor.execute(f"""UPDATE market_statusSET is_open = 1, updated_at = datetime('now')  WHERE market_id = 1""")
                    await message.channel.send(f"Market is now open!")
                    self.market_open = True
                    conn.commit()
                    conn.close()
                elif message.content[8:] == "close" and message.author.id in self.admin_users:
                    conn = sqlite3.connect(self.league_db)
                    cursor = conn.cursor()
                    # Get all the trades that happaned since the market last opened
                    cursor.execute(f"""SELECT updated_at FROM market_status WHERE market_id = 1""")
                    last_market_open = cursor.fetchone()[0]
                    
                    # Get all player in and player out trades per manager
                    trades = cursor.execute(f"""SELECT
                                            trades.trade_id,
                                            requester.manager_name AS requester_name,
                                            requester_player.player_name AS requester_player_name,
                                            requestee_player.player_name AS requestee_player_name
                                        FROM trades
                                        JOIN managers AS requester ON trades.requester_id = requester.manager_id
                                        JOIN managers AS requestee ON trades.requestee_id = requestee.manager_id
                                        JOIN players AS requester_player ON trades.requester_player_id = requester_player.player_id
                                        JOIN players AS requestee_player ON trades.requestee_player_id = requestee_player.player_id
                                        WHERE trades.is_open = FALSE AND is_accepted = TRUE AND updated_at > '{last_market_open}'""")
                    trades = trades.fetchall()
                    columns = ['ID', 'For', 'Player Out', 'Player In']
                    trades_df = pd.DataFrame(trades, columns=columns)
                    trades_df = trades_df.head(5)
                    trades_table = tabulate(trades_df, headers='keys', tablefmt="simple_outline", showindex="never")
                    embed = discord.Embed(title=f"Swaps since last market open: ", color=self.generate_random_color())
                    embed.add_field(name='\u200b', value=f'```\n{trades_table}\n```')
                    await message.channel.send(embed=embed)
                    cursor.execute(f"""UPDATE market_status SET is_open = 0 WHERE market_id = 1""")
                    await message.channel.send(f"Market is now closed!")
                    self.market_open = False
                    conn.commit()
                    conn.close()
                else:
                    await message.channel.send(f"Invalid market command")
                        
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
            cursor.execute(f"""UPDATE closed_game_teams SET is_active = False, updated_at = datetime('now') WHERE player_id = {requester_player_id} and manager_id = {manager_id}""")
            cursor.execute(f"""INSERT into closed_game_teams (manager_id, player_id) VALUES ({manager_id}, {requested_player_id})""")
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
            manager_ids = cursor.execute(f"""SELECT manager_id FROM closed_game_teams WHERE player_id IN (SELECT player_id FROM players WHERE eliminated = TRUE)""")
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
            
            
        # ---------------------------------------------------------------------------------------------------------------------------
        # Hidden commands not supposed to work now
        # Initiate a trade or accept a trade - Not supposed to work
        if message.content.startswith("+trade") and self.market_open == True and 4 == 3:
            self.logger.info(f"Message from {message.author}: {message.content}")
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
            closedCheck = closedCheck.fetchone()
            if closedCheck[0] == False:
                conn.close()
                await message.channel.send(f"You don't need to trade, just swap players using **+swap *ID1* *ID2***")
                return
        
            trade = parse_trade(message.content)
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
                
                # Get the player id of the player being traded for
                query = cursor.execute(f"""SELECT player_id, player_name, role FROM players WHERE player_id = '{requestedplayerid}' or LOWER(player_name) = LOWER('{myplayerid}')""")
                data = query.fetchone()
                requestee_player_id = data[0]
                requestee_player_name = data[1]
                in_player_role = data[2]
                
                if out_player_role != in_player_role:
                    await message.channel.send(f"Can't trade a player with differing roles. Please trade for the same roles")
                    return
                
                # Get the manager id of the manager being traded with
                query = cursor.execute(f"""SELECT cgt.manager_id, m.manager_name, m.discord_user_id
                                            FROM closed_game_teams cgt, managers m
                                            WHERE player_id = '{requestedplayerid}' and is_active = TRUE AND cgt.manager_id = m.manager_id""")
                data = query.fetchone()
                if data is not None:
                    requestee_id = data[0]
                    requestee_name = data[1]
                    requestee_discord_id = data[2]
                    user = await self.fetch_user(requestee_discord_id)
                    
                    # Insert the trade into the trades table
                    cursor.execute(f"""INSERT INTO trades (requester_id, requestee_id, requester_player_id, requestee_player_id) VALUES ({manager_id}, {requestee_id}, {requester_player_id}, {requestee_player_id})""")    
                    trade_id = cursor.lastrowid
                    await message.channel.send(f"Trade request sent to {requestee_name} for {requestee_player_name}")
                    await user.send(f'Trade request with ID: {trade_id} for {requestee_player_name} from {message.author.name}. To accept, use **+trade accept *ID***')
                else:
                    print("Player not on a team, swap complete")
                    
                    cursor.execute(f"""INSERT INTO closed_game_teams (manager_id, player_id) VALUES ({manager_id}, {requestee_player_id})""")
                    cursor.execute(f"""INSERT INTO trades (requester_id, requester_player_id, requestee_player_id, is_accepted, is_open) VALUES 
                                ({manager_id}, {requester_player_id}, {requestee_player_id}, TRUE, FALSE)""")
                    cursor.execute(f"""UPDATE closed_game_teams SET is_active = False WHERE player_id = {requester_player_id} and manager_id = {manager_id}""")
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
                cursor.execute(f"""UPDATE closed_game_teams SET is_active = False 
                                        WHERE player_id = {data[2]} and manager_id = {data[0]}""")
                cursor.execute(f"""UPDATE closed_game_teams SET is_active = False 
                                        WHERE player_id = {data[3]} and manager_id = {data[1]}""")
                
                # Insert new players into both teams
                cursor.execute(f"""INSERT INTO closed_game_teams (manager_id, player_id) VALUES ({data[0]}, {data[3]})""")
                cursor.execute(f"""INSERT INTO closed_game_teams (manager_id, player_id) VALUES ({data[1]}, {data[2]})""")
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


    # Start the bot
    def run(self):
        super().run(self.config["discord_token"], reconnect=True)