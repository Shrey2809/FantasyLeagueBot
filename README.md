# Fantasy Bot

## Introduction

Fantasy Bot is a Discord bot designed to enhance the gaming experience within a fantasy league community. It provides various features and commands related to player statistics, team management, and league activities.

## Usage and Features
### League Independent Commands
1. **Team Management Commands:**
   - Use **+myteam** to view your current in-game team roster.
   - Use **+myscore** to check your scores for previous days in the league.
2. **Player Information Command:**
   - Use **+find *player_name/team_name*** to search for a specific player or team in the league.
   - Use **+openplayers** to get the top 5 open fraggers and supports.
3. **Managers Information Command:**
   - Use **+standings * /open/closed*** to get the standings for the two leagues

### Closed League Comands - Commands that are specific to the closed league
1. **Trade Requests Commands:**
   - Use **+mytrades** to view your current open trade requests (both sent and received).
   - Use **+trade accept *TradeID*** to accept a trade request using the specified trade ID.
   - Use **+trade request *MyPlayerID* *RequestedPlayerID*** to create a trade request.
2. **Player Information Command:**
   - Use **+openplayers** to get the top 5 open fraggers and supports.

### Open League Commands - Commands that are specific to the open league
1. **Team Management Commands**
   - Use **+signup** to sign up to a closed league
   - Use **+pick *PlayerID/PlayerName*** to pick up a player. [Limited to 3 fraggers and 2 supports]
   - Use **+swap *MyPlayerID* *RequestedPlayerID*** to swap your player for another player

### Utility Commands
   - Use **+upload \<file>** to upload a new file to the server, typically used for statistical data.
   - Use **+market open/close** to open or close the market, only accesisble to admins

## Developer Information

Fantasy Bot is developed using Discord.py and various Python libraries. It includes features for managing fantasy league teams, tracking player statistics, and facilitating trade activities within the Discord server.

