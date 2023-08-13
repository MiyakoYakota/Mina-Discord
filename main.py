import discord
import requests
import json
import time

knownUserIDs = {}
knownChannelIDs = {}

class MyClient(discord.Client):
    async def on_ready(self):
        print('Logged on as', self.user)

    # On any socket message
    async def on_socket_response(self, msg):
        print(msg)

    async def on_message(self, discordMessage):
        try:
            # Check if message is from self
            if discordMessage.author.id != self.user.id:
                return
        
            # Check if the message starts with $query
            if discordMessage.content.startswith("$query"):
                # Get the core
                core = discordMessage.content.split(' ')[1]

                params = discordMessage.content.split(' ')[2]

                if core == 'user':
                    core = 'DiscordUsers'
                elif core == 'message':
                    core = 'DiscordMessages'
                elif core == 'discord':
                    core = 'MinaDiscord'
                
                # Get the query
                query = ' '.join(discordMessage.content.split(' ')[3:])

                if 'wt=' not in params:

                    # query upstream
                    r = requests.get(f'http://10.0.5.1:8981/solr/{core}/select?q={query}&indent=true&wt=csv&{params}')
                else:
                    r = requests.get(f'http://10.0.5.1:8981/solr/{core}/select?q={query}&indent=true&{params}')

                # Send the response (truncated to 2000 characters)
                outmessage = "```" + '\n\n'.join(r.text.replace('```', '[TILDA]').split('\n'))[:1990] + "```"
                await discordMessage.channel.send(outmessage)
            
            if discordMessage.content.startswith('$getloc'):
                
                wayback = discordMessage.content.split(' ')[1]
                userid = discordMessage.content.split(' ')[2]


                query = f"(user.id:{userid} OR author.id:{userid} OR user_id:{userid} OR id:{userid} OR discord_id:{userid}) AND mina_type:message AND content:*"

                solrRes = requests.get('http://10.0.5.1:8981/solr/MInaDiscord/select', params={
                    'q': query, 
                    'rows': wayback
                }).json()

                print(solrRes)

                out = {}

                for message in solrRes['response']['docs']:
                    try:
                        content = message['content'][0]
                        data = requests.post('http://127.0.0.1:5000/geoparse', json={'text': content}, timeout=5).json()

                        if len(data) > 0:
                            for location in data:
                                if 'country_conf' in location:
                                    if location['country_conf'] >= 0.7:
                                        country = location['country_predicted']

                                        if country not in out:
                                            out[country] = {
                                                "Mentions": 0,
                                                "Locations": [],
                                                "Original": []
                                            }

                                        out[country]['Mentions'] += 1
                                        if 'geo' in location:
                                            if 'place_name' in location['geo']:
                                                if location['geo']['place_name'] not in out[country]['Locations']:
                                                    out[country]['Locations'].append(location['geo']['place_name'])
                                        out[country]['Original'].append(content[:100])
                    except KeyboardInterrupt:
                        break
                    except Exception as e:
                        print(message['content'])
                        print(e)

                outMsg = json.dumps(out, indent=2)
                await discordMessage.channel.send(outMsg[:3500])
        except Exception as e:
            print(e)
            return

client = MyClient()
client.run('token')
