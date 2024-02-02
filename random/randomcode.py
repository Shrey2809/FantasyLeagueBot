        # Not required at the moment      
        if message.content.startswith("!startdraft"):
            pass
            # file_name = f"draft_order//closed_game.txt"
            # with open(file_name, "r") as f:
            #     draft_order = f.readlines()
            # for i, user in enumerate(draft_order):
            #     draft_order[i] = user.strip()
           
            
            # round_number = 1
            # total_rounds = 5

            # while round_number <= total_rounds:
            #     await message.channel.send(f"**Round {round_number} is starting!**")

            #     # Determine the order based on the round
            #     if round_number % 2 == 0:
            #         order = draft_order[::-1]
            #         type = "support"
            #     else:
            #         order = draft_order
            #         type = "fragger"

            #     for user in order:
            #         await message.channel.send(f"{user}'s turn to pick a {type}!")

            #         # Wait for the user to send their pick
            #         def check(m):
            #             # return m.author == message.author and m.channel == message.channel
            #             return True

            #         try:
            #             pick = await self.wait_for('message', timeout=60.0, check=check)
            #             await message.channel.send(f"{user} picked {pick.content}!")
            #         except asyncio.TimeoutError:
            #             await message.channel.send(f"{user} timed out. Skipping.")
            #             continue

            #     round_number += 1
                    