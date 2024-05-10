import discord
from majorFantasyBotBackend import fantasyBotBackend
import sys
import json

with open('etc/config.json') as config_file:
    config = json.load(config_file)

bot = fantasyBotBackend(config)
bot.run()