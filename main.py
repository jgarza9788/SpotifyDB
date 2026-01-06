#!/usr/bin/env python
# coding: utf-8

# # SpotifyDB Notebook
# This notebook demonstrates setting up and using the SpotifyDB utilities.

# In[ ]:


#in powershell
# %pip install spotipy duckdb python-dotenv pandas


# ## create cred.py  in the root directory 
# add your own client id and secrets using the values you have optained from spotify.
# 
# 
# ### cred.py
# ```python
# # get values from 
# # https://developer.spotify.com/dashboard/
# spotify_client_id = "your id"
# spotify_client_secret = "your secret"
# ```
# 

# In[2]:


# import the credentials from cred.py
from cred import spotify_client_id, spotify_client_secret


# ## import libraries

# In[ ]:


import os, time
# from dotenv import load_dotenv

import duckdb
import pandas as pd

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# custom functions
import functions as fn

from IPython.display import display


# ## setting up the environment, api, and duckdb

# In[4]:


# Load .env
# load_dotenv()

CLIENT_ID = spotify_client_id
CLIENT_SECRET = spotify_client_secret
REDIRECT_URI = "http://127.0.0.1:8000/callback"
SCOPE = (
    "user-library-read user-read-recently-played user-top-read user-read-playback-state user-follow-read playlist-read-private playlist-modify-private playlist-modify-public"
)

assert CLIENT_ID and CLIENT_SECRET and REDIRECT_URI, "Missing Spotify env vars"

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        open_browser=True,  # will open auth page in browser
        cache_path=".cache-spotifydb"  # token cache
    )
)

current_user = sp.current_user()
current_user["display_name"], current_user["id"]


# In[5]:


# In-memory DB (great for playing around)
# con = duckdb.connect(database=':memory:')

# If you want persistent on-disk:
con = duckdb.connect(database='spotify.duckdb')


# In[ ]:


df_tables = con.execute(f"SHOW TABLES").df()
display(df_tables)


# ## function wrapper 

# In[ ]:


def get_item(table_name:str,source_type:str, source_id:str,verbose=True):
    """
    source_type: playlist, album, artist, liked_songs, top_tracks, recently_played
    """
    global con
    global sp

    table_age = fn.duckdb_table_age(con, table_name)    
    print(f"table age: {table_age} days old")

    if table_age is None or table_age > 1.0:

        if source_type == "playlist":
            items = fn.load_tracks_from_playlist(sp, source_id)
        elif source_type == "album":
            items = fn.load_tracks_from_album(sp, source_id)
        elif source_type == "artist":
            items = fn.load_tracks_from_artist(sp, source_id)
        elif source_type == "liked_songs":
            items = fn.load_my_saved_tracks(sp)
        elif source_type == "top_tracks":
            items = fn.load_my_top_tracks(sp)
        elif source_type == "recently_played":
            # recently_played tracks 
            items = fn.load_my_recently_played(sp)
        # elif source_type == "recently_played_last3M":
        #     # recently played tracks in the last 3 months
        #     now = int(time.time() * 1000)
        #     three_months_ago = now - (90 * 24 * 60 * 60 * 1000)
        #     items = fn.load_my_recently_played(sp, after=three_months_ago)
        # elif source_type == "one_year_ago":
        #     # stuff i haven't played in over a year
        #     now = int(time.time() * 1000)
        #     one_year_ago = now - (365 * 24 * 60 * 60 * 1000)
        #     items = fn.load_my_recently_played(sp, before=one_year_ago)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")

        try:
            fn.df_to_duckdb(con, items, table_name)
        except Exception as e:
            print(f"Error saving to DuckDB: {e}")
    else:
        items = fn.duckdb_to_df(con, table_name)

    if verbose:
        display(f"{table_name}: {len(items)} records")
        display(items.head())

    return items




# ## geting basic data 

# ### my followed artist 

# In[ ]:


table_age = fn.duckdb_table_age(con, "followed_artist")

print(f"table age: {table_age} days old")


if table_age is None or table_age > 1.0:
    print("Refreshing followed_artist table...")

    followed_artist = fn.get_followed_artists_df(sp)
    display(followed_artist.head())

    print("Loading followed_artist table from DuckDB...")
    fn.df_to_duckdb(con, followed_artist, "followed_artist")
else:
    followed_artist = fn.duckdb_to_df(con, "followed_artist")
    print("Loaded followed_artist table from DuckDB.")


display(f"Followed Artists: {len(followed_artist)} records")
display(followed_artist.head())


# ### my liked songs

# In[9]:


my_liked_songs = get_item("my_liked_songs", "liked_songs", None)


# ## new_liked_songs

# In[ ]:


new_liked_songs = duckdb.query(
"""
select * from my_liked_songs 
order by saved_at 
desc limit 100
"""
).to_df()


display(f"new_liked_songs: {len(new_liked_songs)}")
display(new_liked_songs.head())


# let's save this as a Playlist on Spotify
playlist_id = fn.create_playlist(
    sp,
    name="**New Liked Songs",
    description="My 100 most recently liked songs, updated via Spotify API",
    public=True,
    overwrite_if_exists=True
)

fn.add_tracks_to_playlist(sp, playlist_id, new_liked_songs["uri"].tolist())


# ## cream of crop

# In[ ]:


cream_of_crop = duckdb.query(
"""
select * from my_liked_songs 
order by popularity 
desc limit 100
"""
).to_df()


display(f"cream_of_crop: {len(cream_of_crop)}")
display(cream_of_crop.head())

# let's save this as a Playlist on Spotify
playlist_id = fn.create_playlist(
    sp,
    name="**Cream of Crop",
    description="My 100 most popular liked songs, updated via Spotify API",
    public=True,
    overwrite_if_exists=True
)

fn.add_tracks_to_playlist(sp, playlist_id, cream_of_crop["uri"].tolist())


# ## discover_these 

# In[ ]:


top_artist = followed_artist.head(10)['artist_id'].to_list()

disc_these = pd.DataFrame()
for artist_id in top_artist:
    artist_albums = fn.get_artist_top_tracks_df(sp, artist_id)
    disc_these = pd.concat([disc_these, artist_albums], ignore_index=True)


display(f"disc_these: {len(disc_these)}")
display(disc_these.head())


# let's save this as a Playlist on Spotify
playlist_id = fn.create_playlist(
    sp,
    name="**discover these",
    description="Best songs from recently followed artist, updated via Spotify API",
    public=True,
    overwrite_if_exists=True
)

fn.add_tracks_to_playlist(sp, playlist_id, disc_these["uri"].tolist())


# ## let's mix some playlists
# 
# ### Covers++

# In[ ]:


# load some playlists
Covers = get_item("Covers", "playlist", "6jfY6NVENX592ZhLizN4HO",verbose=False)
AI_Covers = get_item("AI_Covers", "playlist", "5xooQuxBYK7ZXN4dhSQ9GL",verbose=False)
NTS_Covers = get_item("NTS_Covers", "playlist", "53pyL7jy1hbFbttiZZ8g1D",verbose=False)

# sort by popularity
Covers = Covers.sort_values(by=['popularity'], ascending=False).reset_index(drop=True)
AI_Covers = AI_Covers.sort_values(by=['popularity'], ascending=False).reset_index(drop=True)
NTS_Covers = NTS_Covers.sort_values(by=['popularity'], ascending=False).reset_index(drop=True)

# combine, drop duplicates, re-sort
CoversPP = pd.concat(
    [
        Covers.head(60), 
        AI_Covers.head(60),
        NTS_Covers.head(20)
    ], 
    ignore_index=True
    )
CoversPP = CoversPP.drop_duplicates()
CoversPP = CoversPP.sort_values(by=['popularity'], ascending=False).reset_index(drop=True)

display(f"CoversPP: {len(CoversPP)}")
display(CoversPP.head())

# save the playlist 
playlist_id = fn.create_playlist(
    sp,
    name="**Covers ++",
    description="Some of the Best Covers from my picks and AI, updated via Spotify API",
    public=True,
    overwrite_if_exists=True
)

fn.add_tracks_to_playlist(sp, playlist_id, CoversPP["uri"].tolist())


# #### this is just a more sql way of doing the same thing
# * similar output
# 
# ```python 
# 
# 
# # load some playlists
# Covers = get_item("Covers", "playlist", "6jfY6NVENX592ZhLizN4HO",verbose=False)
# AI_Covers = get_item("AI_Covers", "playlist", "5xooQuxBYK7ZXN4dhSQ9GL",verbose=False)
# NTS_Covers = get_item("NTS_Covers", "playlist", "53pyL7jy1hbFbttiZZ8g1D",verbose=False)
# 
# 
# # display(NTS_Covers.head())
# 
# CoversPP = duckdb.query(
# """
# 
# select 
# distinct * from 
# (
# 
#     select * from 
#     (
#         select * from Covers
#         order by popularity desc 
#         limit 60
#     ) A
# 
#     union all 
# 
#     select * from     
#     (    
#         select * from AI_Covers 
#         order by popularity desc 
#         limit 60
#     ) B
# 
#     union all
# 
#     select * from     
#     (    
#         select * from NTS_Covers 
#         order by popularity desc 
#         limit 20
#     ) C
# 
# ) D
# order by popularity 
# desc limit 150
# 
# """
# ).to_df()
# 
# 
# display(f"CoversPP: {len(CoversPP)}")
# display(CoversPP.head())
# 
# # save the playlist 
# playlist_id = fn.create_playlist(
#     sp,
#     name="**Covers ++",
#     description="Some of the Best Covers from my picks and AI, updated via Spotify API",
#     public=True,
#     overwrite_if_exists=True
# )
# 
# fn.add_tracks_to_playlist(sp, playlist_id, CoversPP["uri"].tolist())
# ```

# ## forgotten tracks

# In[ ]:


recently_played = get_item("recently_played", "recently_played", None, verbose=False)

forgotten_tracks = duckdb.query(
"""

select * from 
(
    select 
    distinct 
    * from my_liked_songs  mls
    left join recently_played rp
    on rp.track_id = mls.track_id
    where rp.track_id is null
    order by saved_at asc --saved a long time ago
    limit 500
)
order by random()
limit 150

"""
).to_df()


display(f"forgotten_tracks: {len(forgotten_tracks)}")
display(forgotten_tracks.head())

# save the playlist 
playlist_id = fn.create_playlist(
    sp,
    name="**Forgotten Tracks",
    description="Some of my liked tracks that i haven't listened to in a while, updated via Spotify API",
    public=True,
    overwrite_if_exists=True
)

fn.add_tracks_to_playlist(sp, playlist_id, CoversPP["uri"].tolist())


# ## Mix 182

# In[15]:


# # load some playlists
# Blink182 = get_item("Blink182", "artist", "6FBDaR13swtiWwGhX1WQsP",verbose=False)
# TheParadox = get_item("TheParadox", "artist", "6GhcI55xfZf5vqmmNqYzxW",verbose=False)
# MagnoliaPark = get_item("MagnoliaPark", "artist", "7B76SsfzG0wWk1WEvGzCmY",verbose=False)
# Sum41 = get_item("Sum41", "artist", "0qT79UgT5tY4yudH9VfsdT",verbose=False)
# All_American_Rejects = get_item("All_American_Rejects", "artist", "spotify:artist:3vAaWhdBR38Q02ohXqaNHT",verbose=False)

# dfl = [
#     Blink182,
#     TheParadox,
#     MagnoliaPark,
#     Sum41,
#     All_American_Rejects
# ]

# for temp_df in dfl:
#     temp_df = temp_df.sort_values(by=['popularity'], ascending=False).reset_index(drop=True)

# MIX182 = pd.DataFrame()

# for temp_df in dfl:
#     MIX182 = pd.concat(
#         [
#             MIX182,
#             temp_df.head(30)
#         ],
#         ignore_index=True
#     )

# MIX182 = MIX182.drop_duplicates()
# MIX182 = MIX182.sort_values(by=['popularity'], ascending=False).reset_index(drop=True)

# display(f"MIX182: {len(MIX182)}")
# display(MIX182.head())

# # save the playlist 
# playlist_id = fn.create_playlist(
#     sp,
#     name="**MIX182",
#     description="A Mix of Blink-182 and other Bands, updated via Spotify API",
#     public=True,
#     overwrite_if_exists=True
# )

# fn.add_tracks_to_playlist(sp, playlist_id, MIX182["uri"].tolist())

