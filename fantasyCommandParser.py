import re

def parse_trade(command):
    # Define the pattern for the trade request command
    request_pattern = r'\+trade\s+request\s+(\d+)\s+(\d+)'
    accept_pattern = r'\+trade\s+accept\s+(\d+)'
    pattern_match = re.match(request_pattern, command)
    if pattern_match:
        # Extract player IDs from the matched groups
        requester_player_id, requestee_player_id = map(int, pattern_match.groups())

        # Return a tuple containing the parsed player IDs
        return {'Type': 'request', 'MyPlayer': requester_player_id, 'TradeFor': requestee_player_id}
    elif accept_pattern:
        # Extract player IDs from the matched groups
        request_id = int(re.match(accept_pattern, command).group(1))

        # Return a tuple containing the parsed player IDs
        return {'Type': 'accept', 'TradeID': request_id}
    else:
        # Return None if the command doesn't match the expected pattern
        return None
    
def parse_swap(command):
    # Define the pattern for the trade request command
    request_pattern = r'\+swap\s+(\d+)\s+(\d+)'
    pattern_match = re.match(request_pattern, command)
    if pattern_match:
        # Extract player IDs from the matched groups
        requester_player_id, requestee_player_id = map(int, pattern_match.groups())

        # Return a tuple containing the parsed player IDs
        return {'Type': 'request', 'MyPlayer': requester_player_id, 'TradeFor': requestee_player_id}
    else:
        # Return None if the command doesn't match the expected pattern
        return None

