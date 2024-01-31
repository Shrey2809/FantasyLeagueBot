# Fantasy Bot

## Introduction

Fantasy Bot is a versatile Discord bot designed to enrich the gaming experience within fantasy league communities. It offers an array of features and commands tailored to player statistics, team management, and league activities.

## Usage and Features

### General Commands

1. **Team Management:**
   - **+myteam**: View your current in-game team roster.
   - **+myscore**: Check your scores for previous days in the league.

2. **Player Information:**
   - **+find *player_name/team_name***: Search for a specific player or team in the league.
   - **+openplayers**: Discover the top 5 open fraggers and supports.

3. **Managerial Information:**
   - **+standings *open/ /closed***: Access standings for the two leagues.

### Closed League Commands

1. **Requests:**
   - **+myrequests**: View your current open request.
   - **request *MyPlayerID/Name* *RequestedPlayerID/Name***: Create a trade request.

2. **Player Information:**
   - **+openplayers**: Access top 5 open fraggers and supports.

### Open League Commands

1. **Team Management:**
   - **+signup**: Sign up for a closed league.
   - **+pick *PlayerID/PlayerName***: Pick up a player (Limited to 3 fraggers and 2 supports).
   - **+swap *MyPlayerID/Name* *RequestedPlayerID/Name***: Swap your player for another.

### Utility Commands
   - **+upload \<file>**: Upload a new file to the server, typically used for statistical data.
   - **+market open/close**: Open or close the market (accessible only to admins).

## Developer Information

Fantasy Bot is developed using Discord.py and various Python libraries. It includes features for managing fantasy league teams, tracking player statistics, and facilitating trade activities within the Discord server.