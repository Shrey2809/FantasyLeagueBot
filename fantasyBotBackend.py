import discord
from discord.ext import commands
import logging
import aiohttp
import json
import pandas as pd
import random
import csv
import re
import sqlite3
import datetime
import asyncio
from tabulate import tabulate

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

        self.admin_users = ["axon319", "jessejchick", "sprabuni"]
        self.league_db = "SI_2024_FANTASY_LEAGUE.db"

        super().__init__(command_prefix="+help", status=discord.Status.online, intents=discord.Intents.all())

        # Configure the logger
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(levelname)s] %(message)s',
            filename='fantasyLeague.log',  # Specify the path to your log file
            filemode='a'  # Use 'a' to append to the file, 'w' to overwrite
        )
        self.logger = logging.getLogger(__name__)
    
    def generate_random_color(self):
        color = random.randint(0, 0xFFFFFF)  # 0x000000 to 0xFFFFFF (0 to 16777215 in decimal)
        return color
    
    
    # Message displayed when bot is started
    async def on_ready(self):
        self.http_session = aiohttp.ClientSession()
        self.logger.info("-------------")
        self.logger.info("|Fantasy Bot|")
        self.logger.info("-------------")

    async def on_message(self, message):
        # ---------------------------------------------------------------------------------------------------------------------------
        # Get my current action team
        if message.content.startswith("+myteam"):
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
        
        # Get all my past scores for previous days
        if message.content.startswith("+myscore"):
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            # Execute the SQL query and fetch the data into a Pandas DataFrame
            closedCheck = cursor.execute(f"""SELECT in_closed FROM managers WHERE discord_user_id = {userID}""")
            closedCheck = closedCheck.fetchone()
            if closedCheck[0] == True:
                query = cursor.execute(f"""
                    SELECT 
                        date, 
                        closed_game_score 
                    FROM managers m
                    JOIN manager_daily_scores mds ON m.manager_id = mds.manager_id
                    WHERE m.manager_id = (SELECT m2.manager_id FROM managers m2 WHERE m2.discord_user_id = '{userID}')
                """)
                data = query.fetchall()
            else:
                query = cursor.execute(f"""
                    SELECT 
                        date, 
                        open_game_score 
                    FROM managers m
                    JOIN manager_daily_scores mds ON m.manager_id = mds.manager_id
                    WHERE m.manager_id = (SELECT m2.manager_id FROM managers m2 WHERE m2.discord_user_id = '{userID}')
                """)
                data = query.fetchall()

            # Create a DataFrame with the fetched data
            columns = ['Date', 'Score']
            df = pd.DataFrame(data, columns=columns)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"{message.author.name}'s scores: ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
                
            conn.close()
        
        # Get top 5 open fraggers and supports and send embed
        if message.content.startswith("+openplayers"):
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

        # Get my current open trades both sent and recieved
        if message.content.startswith("+mytrades"):
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            
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
        
        # Find a specific player
        if message.content.startswith("+find"):
            userID = message.author.id
            conn = sqlite3.connect(self.league_db)
            cursor = conn.cursor()
            query = cursor.execute(f"""SELECT
                                            p.player_id,
                                            p.player_name,
                                            p.team_name,
                                            p.role,
                                            p.region,
                                            p.eliminated,
                                            p.is_active
                                        FROM players p
                                        WHERE LOWER(p.player_name) LIKE LOWER('%{message.content[6:]}%')""")
            data = query.fetchall()
            columns = ['ID', 'Name', 'Team', 'Role', 'Region', 'Eliminated', 'Active']
            df = pd.DataFrame(data, columns=columns)
            table = tabulate(df, headers='keys', tablefmt="simple_outline", showindex="never")
            embed = discord.Embed(title=f"Search results for '{message.content[6:]}': ", color=self.generate_random_color())
            embed.add_field(name='\u200b', value=f'```\n{table}\n```')
            await message.channel.send(embed=embed)
            conn.close()
        
        # ---------------------------------------------------------------------------------------------------------------------------
        # Utility commands
        # Upload a new file to the server
        if (message.content.startswith("+upload") or message.content.startswith("+d") 
         or (message.channel.id == 1198778760120500284 and message.guild.id == 1042862967072501860)):
            if message.attachments:
                current_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M")
                file_name = f'stats//RAW_TOTALS_{current_datetime}.csv'

                for attachment in message.attachments:
                    await attachment.save(file_name)
                    print(f"File '{attachment.filename}' downloaded from {message.author}. Saved as {file_name}.")  
            
          
             


    # Start the bot
    def run(self):
        super().run(self.config["discord_token"], reconnect=True)