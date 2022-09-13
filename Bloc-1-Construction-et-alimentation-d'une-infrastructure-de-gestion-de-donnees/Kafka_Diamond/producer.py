# Code inspired from Confluent Cloud official examples library
# https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/producer.py

from confluent_kafka import Producer
import json
import ccloud_lib # Library not installed with pip but imported from ccloud_lib.py
import numpy as np
import time
from datetime import datetime
import requests
from riotwatcher import LolWatcher, ApiError
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "diamond_matches" 

# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
producer = Producer(producer_conf)

# Create topic if it doesn't already exist
ccloud_lib.create_topic(CONF, TOPIC)

try:

    # url = "https://realstonks.p.rapidapi.com/TSLA"

    # headers = {
    #     "X-RapidAPI-Key": "17ab4668f1msh45ada6f47d2d8b5p1f68a0jsnbf4eb5774a57",
    #     "X-RapidAPI-Host": "realstonks.p.rapidapi.com"
    # }

    api_key = os.getenv('DIAMOND_API_KEY')
    watcher = LolWatcher(api_key)
    region = 'euw1'
    game_mode = "RANKED_SOLO_5x5"
    diamond = "DIAMOND"
    diamond_division = "I"
    diamond_players = watcher.league.entries(region, game_mode, diamond, diamond_division)

    diamond_players_lst = []

    for i in diamond_players:
        diamond_players_lst.append(i["summonerId"])

    if os.path.exists("diamond_match_list.txt"):
        with open("diamond_match_list.txt", "r") as f:
            full_diamond_matches_lst = f.readlines()
            full_diamond_matches_lst = [i.strip() for i in full_diamond_matches_lst]
    else:
        full_diamond_matches_lst = []


    # Starts an infinite while loop that produces random current temperatures
    while True:

        puuid_lst = []

        for i in range(len(diamond_players_lst)):
            time.sleep(1.3)
            puuid_lst.append(watcher.summoner.by_id(region, diamond_players_lst[i])['puuid'])
            print (f"Diamond players puuid saved so far: {i+1}/{len(diamond_players_lst)}")

        matches_lst = []
        counter = 0

        for i in puuid_lst:
            time.sleep(1.3)
            matches_lst.append(watcher.match.matchlist_by_puuid(region, i, 0, 100, start_time=1641340800))
            counter+=1
            print (f"Diamond players matches list so far: {counter}/{len(puuid_lst)}")


        matches_lst = [x for xs in matches_lst for x in xs]
        matches_lst = list(set(matches_lst))

        soloqueue_counter = 0
        other_counter = 0
        total_counter = len(full_diamond_matches_lst)
        match_added_this_session = 0
        match_already_in_list = 0

        for i in matches_lst:
            print (f"Total diamond matches saved: {total_counter}", end = ' ')
            if i not in full_diamond_matches_lst:
                full_diamond_matches_lst.append(i)
                time.sleep(1.3)
                total_counter+=1
                match_added_this_session+=1
                try:
                    match_detail = watcher.match.by_id(region, i)
                    if match_detail['info']["queueId"] == 420:
                        participants = []
                        soloqueue_counter +=1
                        print (f"-- Ranked matches saved this session: {soloqueue_counter}/{match_added_this_session}")
                        for row in match_detail['info']['participants']:
                            record_key = "lol"
                            record_value = json.dumps(
                                {
                                "champion": row['championName'],
                                "position": row['individualPosition'],
                                "win": row['win'],
                                "kills": row['kills'],
                                "deaths": row['deaths'],
                                "assists": row['assists'],
                                "totalDamageDealtToChampions": row['totalDamageDealtToChampions'],
                                "goldPerMinute": row["challenges"]['goldPerMinute'],
                                "champLevel": row['champLevel'],
                                "laneMinionsFirst10Minutes": row["challenges"]['laneMinionsFirst10Minutes'],
                                "jungleCsBefore10Minutes": row["challenges"]['jungleCsBefore10Minutes'],
                                "outerTurretExecutesBefore10Minutes": row["challenges"]['outerTurretExecutesBefore10Minutes'],
                                "visionScore": row['visionScore'],
                                "visionScoreAdvantageLaneOpponent": row["challenges"]['visionScoreAdvantageLaneOpponent'],
                                "pentaKills": row['pentaKills'],
                                "perfectDragonSoulsTaken": row["challenges"]['perfectDragonSoulsTaken'],
                                "gameVersion": match_detail['info']['gameVersion'],
                                "gameId": i
                                }
                            )
                            # time.sleep(1.3)
                            producer.produce(
                                TOPIC,
                                key=record_key,
                                value=record_value,
                            )
                    else:                 
                        other_counter+=1
                        print (f"-- Non-ranked match saved this session: {other_counter}/{match_added_this_session}")
                        pass    
                except:
                    print ("-- Unable to gather data for unknown reason")
                    pass
            
            else:
                match_already_in_list+=1
                print (f"-- Number of matches that were already in list: {match_already_in_list}")
                continue


        # record_key = "tesla"
        # record_value = json.dumps(
        #     {
        #         "current_price": data["price"],
        #         "time_of_price": str(now)
                
        #     }
        # )
        # print("Producing record: {}\t{}".format(record_key, record_value))

        # This will actually send data to your topic


 # Interrupt infinite loop when hitting CTRL+C
except KeyboardInterrupt:
    pass
finally:
    with open("diamond_match_list.txt", "w") as f:
        f.writelines(i + '\n' for i in full_diamond_matches_lst)
    
    producer.flush() # Finish producing the latest event before stopping the whole script