import glob
import ndjson
import json
import pandas as pd
from datetime import datetime

files = glob.glob("*.ndjson")

list_of_frames = []

for file in files:
    with open(file) as f:
        data = ndjson.load(f)
        for d in data:

            # Generate de DICTIONARY
            all_data = {}

            # BASIC METADATA
            all_data["meta_item_id"] = d["item_id"]
            timestamp_created_at = int(d["data"]["createTime"])
            all_data["meta_createTime"] = datetime.fromtimestamp(timestamp_created_at).strftime('%d-%m-%Y %H:%M:%S')
            timestamp_collected = d["timestamp_collected"]
            all_data["meta_date_collected"] = datetime.fromtimestamp(timestamp_collected/1000).strftime('%d-%m-%Y %H:%M:%S')
            all_data["meta_source_platform"] = d["source_platform"]
            all_data["meta_source_url"] = d["source_url"]
            all_data["meta_is_Ad"] = d["data"]["isAd"]
            try:
                all_data["meta_labels"] = d["data"]["diversificationLabels"]
            except KeyError:
                all_data["meta_labels"] = "none"

            all_data["meta_locationCreated"] = d["data"]["locationCreated"]

            # AUTHOR AND TEXT
            all_data["author_nickname"] = d["data"]["author"]
            all_data["author_id"] = d["data"]["authorId"]
            all_data["author_followerCount"] = d["data"]["authorStats"]["followerCount"]
            all_data["author_followingCount"] = d["data"]["authorStats"]["followingCount"]
            all_data["author_heart"] = d["data"]["authorStats"]["heart"]
            all_data["author_heartCount"] = d["data"]["authorStats"]["heartCount"]
            all_data["author_videoCount"] = d["data"]["authorStats"]["videoCount"]
            all_data["author_diggCount"] = d["data"]["authorStats"]["diggCount"]

            # METRICS
            all_data["text"] = d["data"]["desc"]
            all_data["challenges"] = d["data"]["challenges"]
            all_data["metrics_diggCount"] = d["data"]["stats"]["diggCount"]
            all_data["metrics_shareCount"] = d["data"]["stats"]["shareCount"]
            all_data["metrics_commentCount"] = d["data"]["stats"]["commentCount"]
            all_data["metrics_playCount"] = d["data"]["stats"]["playCount"]
            all_data["metrics_collectCount"] = int(d["data"]["stats"]["collectCount"])
            # MUSIC DATA
            all_data["music_id"] = d["data"]["music"]["id"]
            all_data["music_title"] = d["data"]["music"]["title"]
            all_data["music_author"] = d["data"]["music"]["authorName"]
            all_data["music_original"] = d["data"]["music"]["original"]
            all_data["music_duration"] = d["data"]["music"]["duration"]

            # VIDEO METADATA
            all_data["video_height"] = d["data"]["video"]["height"]
            all_data["video_width"] = d["data"]["video"]["width"]
            all_data["video_ratio"] = d["data"]["video"]["ratio"]
            all_data["video_duration"] = d["data"]["video"]["duration"]
            all_data["video_subtitles"] = d["data"]["video"]["subtitleInfos"]
            all_data["video_audio_loudness"] = d["data"]["video"]["volumeInfo"]["Loudness"]
            all_data["video_audio_peak"] = d["data"]["video"]["volumeInfo"]["Peak"]

            df = pd.DataFrame.from_dict([all_data], orient="columns")
            print(df)

            list_of_frames.append(df)

final_df = pd.concat(list_of_frames)
final_df.to_excel("output.xlsx", index=False)
