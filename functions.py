
import pandas as pd

def normalize_track_item(sp, track, source_type, source_id):
    """
    Convert a Spotify track object into a flat row suitable for DuckDB.
    Includes genres (via artists) and play_count placeholder.
    """
    # --------------------------
    # Construct normalized object
    # --------------------------
    return {
        "source_type": source_type,
        "source_id": source_id,

        "track_id": track["id"],
        "track_name": track["name"],

        "album_name": track["album"]["name"] if track.get("album") else None,
        "album_id": track["album"]["id"] if track.get("album") else None,

        "artist_name": ", ".join(a["name"] for a in track["artists"]),
        "artist_ids": ", ".join(a["id"] for a in track["artists"]),

        "duration_ms": track.get("duration_ms"),
        "explicit": track.get("explicit"),
        "popularity": track.get("popularity"),
        "disc_number": track.get("disc_number"),
        "track_number": track.get("track_number"),
        "uri": track.get("uri"),
    }

def load_tracks_from_playlist(sp, playlist_id, limit:int=100):
    """
    Example:
    df = load_tracks_from_playlist(sp, "37i9dQZF1DXcBWIGoYBM5M")  # Today's Top Hits
    df.head()

    """
    tracks = []
    offset = 0

    while True:
        data = sp.playlist_items(playlist_id, limit=limit, offset=offset)
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            track = item["track"]
            if track and track["id"]:
                tracks.append(normalize_track_item(track, "playlist", playlist_id))

        offset += len(items)

    return pd.DataFrame(tracks)

def load_tracks_from_album(sp, album_id):
    """
    Example:
    df = load_tracks_from_album(sp, "4aawyAB9vmqN3uQ7FjRGTy") 
    df.head()

    """
    album = sp.album(album_id)
    album_name = album["name"]

    tracks = []
    for track in album["tracks"]["items"]:
        # Spotify album tracks lack full "track" object fields unless we fetch
        full_track = sp.track(track["id"])
        tracks.append(normalize_track_item(full_track, "album", album_id))

    return pd.DataFrame(tracks)

def load_tracks_from_artist(sp, artist_id):
    """
    Example:
    df = load_tracks_from_artist(sp, "4dpARuHxo51G3z768sgnrY")  # Adele
    df.head()
    """
    albums = []
    
    # Get all albums + singles
    results = sp.artist_albums(artist_id, album_type="album,single,compilation", limit=50)
    albums.extend(results["items"])

    # Continue paging if needed
    while results.get("next"):
        results = sp.next(results)
        albums.extend(results["items"])

    seen_albums = {a["id"]: a for a in albums}.values()

    all_tracks = []

    # Load tracks from each album
    for album in seen_albums:
        album_id = album["id"]
        data = sp.album_tracks(album_id)
        for item in data["items"]:
            full_track = sp.track(item["id"])
            all_tracks.append(normalize_track_item(full_track, "artist", artist_id))

    # Convert to DataFrame and drop duplicates
    df = pd.DataFrame(all_tracks)
    df = df.drop_duplicates(subset=["track_id"])

    return df

def load_any(sp, spotify_id, id_type=None):
    """
    Universal loader:
      id_type: playlist | album | artist | track
      If omitted, auto-detects based on URI format.

    Example:
    df = load_any(sp, "https://open.spotify.com/playlist/37i9dQZF1DX4JAvHpjipBk")
    df.head()
    """
    
    # If passed a URI, auto-detect
    if spotify_id.startswith("spotify:"):
        parts = spotify_id.split(":")
        id_type = parts[1]
        spotify_id = parts[2]

    # If passed a URL
    elif "open.spotify.com" in spotify_id:
        parts = spotify_id.split("/")
        id_type = parts[-2]
        spotify_id = parts[-1].split("?")[0]

    if not id_type:
        raise ValueError("Must specify id_type (playlist, album, artist, track)")

    if id_type == "playlist":
        return load_tracks_from_playlist(sp, spotify_id)
    if id_type == "album":
        return load_tracks_from_album(sp, spotify_id)
    if id_type == "artist":
        return load_tracks_from_artist(sp, spotify_id)
    if id_type == "track":
        # Wrap one track into a DF
        track = sp.track(spotify_id)
        return pd.DataFrame([normalize_track_item(track, "track", spotify_id)])

    raise ValueError(f"Unsupported id_type: {id_type}")

def load_my_saved_tracks(sp, limit:int=50):
    """
    Fetch all saved (liked) tracks for the current user.

    Parameters
    ----------
    sp : spotipy.Spotify
        An authenticated Spotipy client.
    limit : int, optional
        Page size for each API request (max 50).

    Returns
    -------
    pandas.DataFrame
        One row per saved track with useful metadata.
    """
    results = []
    offset = 0

    while True:
        page = sp.current_user_saved_tracks(limit=limit, offset=offset)
        items = page["items"]
        if not items:
            break

        for item in items:
            track = item["track"]
            if not track:
                continue
            
            # audio_features = get_audio_features(sp, track["id"])

            results.append({
                "saved_at": item["added_at"],
                "track_id": track["id"],
                "track_name": track["name"],
                "album_name": track["album"]["name"],
                "album_id": track["album"]["id"],
                "artist_name": ", ".join(a["name"] for a in track["artists"]),
                "artist_id": track["artists"][0]["id"] if track["artists"] else None,
                "duration_ms": track["duration_ms"],
                "explicit": track["explicit"],
                "popularity": track.get("popularity"),
                "is_local": track.get("is_local", False),
                "uri": track["uri"],

            })

        offset += len(items)

    return pd.DataFrame(results)

# def audio_features_for_tracks(sp,track_ids, batch_size=10):
#     """
#     track_ids = saved_df["track_id"].dropna().unique().tolist()
#     features_df = fetch_audio_features_for_tracks(track_ids)
#     """
#     features_rows = []
    
#     for i in range(0, len(track_ids), batch_size):
#         batch = track_ids[i:i+batch_size]
#         features = sp.audio_features(batch)
#         for f in features:
#             if f is None:
#                 continue
#             features_rows.append({
#                 "track_id": f["id"],
#                 "danceability": f["danceability"],
#                 "energy": f["energy"],
#                 "key": f["key"],
#                 "loudness": f["loudness"],
#                 "mode": f["mode"],
#                 "speechiness": f["speechiness"],
#                 "acousticness": f["acousticness"],
#                 "instrumentalness": f["instrumentalness"],
#                 "liveness": f["liveness"],
#                 "valence": f["valence"],
#                 "tempo": f["tempo"],
#                 "time_signature": f["time_signature"]
#             })
#     return pd.DataFrame(features_rows)

def find_playlist_by_name(sp,name):
    playlists = []
    results = sp.current_user_playlists(limit=50)
    playlists.extend(results["items"])
    while results["next"]:
        results = sp.next(results)
        playlists.extend(results["items"])
    for p in playlists:
        if p["name"].lower() == name.lower():
            return p
    return None

def create_playlist(sp, name, description="", public=False, overwrite_if_exists=False):
    """
    Create a playlist for the authenticated user.
    
    If overwrite_if_exists=True and a playlist with the same name exists,
    it will be cleared and reused.
    """
    user_id = sp.me()["id"]  # ← Get current user ID directly

    # Helper: find playlist by name for this user
    def _find_playlist_by_name(sp, user_id, name):
        results = sp.current_user_playlists(limit=50)
        playlists = results["items"]

        while results.get("next"):
            results = sp.next(results)
            playlists.extend(results["items"])

        for p in playlists:
            if (
                p["name"].lower() == name.lower()
                and p["owner"]["id"] == user_id
            ):
                return p
        return None

    existing = _find_playlist_by_name(sp, user_id, name)

    if existing and overwrite_if_exists:
        playlist_id = existing["id"]

        # Clear tracks
        sp.playlist_replace_items(playlist_id, [])

        # Update metadata
        sp.playlist_change_details(
            playlist_id,
            name=name,
            public=public,
            description=description
        )

        return playlist_id

    # Otherwise create a brand-new playlist
    playlist = sp.user_playlist_create(
        user=user_id,
        name=name,
        public=public,
        description=description
    )
    return playlist["id"]

def add_tracks_to_playlist(sp,playlist_id, uris, chunk_size=100):
    """
    example:
    add_tracks_to_playlist(sp, playlist_id, workout_df["uri"].tolist())
    """
    for i in range(0, len(uris), chunk_size):
        sp.playlist_add_items(playlist_id, uris[i:i+chunk_size])

def playlist_cleanup(sp, playlist_id, sort_by=None):
    """
    Clean up a playlist:
      - Remove duplicates
      - Remove local/unavailable tracks
      - Optionally sort tracks

    Example:
    playlist_id = "37i9dQZF1DXcBWIGoYBM5M"

    clean_df = playlist_cleanup(
        sp,
        playlist_id,
        sort_by="artist_name"
    )

    clean_df.head()
    """
    # Fetch items
    results = []
    offset = 0
    limit = 100

    while True:
        page = sp.playlist_items(playlist_id, offset=offset, limit=limit)
        items = page.get("items", [])
        if not items:
            break

        for item in items:
            track = item.get("track")
            if track:
                results.append(track)

        offset += len(items)

    # Normalize to DataFrame
    df = pd.DataFrame([
        {
            "track_id": t["id"],
            "track_name": t["name"],
            "artist_name": ", ".join(a["name"] for a in t["artists"]),
            "album_name": t["album"]["name"],
            "explicit": t["explicit"],
            "popularity": t["popularity"],
            "is_local": t["is_local"],
            "uri": t["uri"],
        }
        for t in results
        if t and t.get("id")
    ])

    # Remove local files
    df = df[df["is_local"] == False]

    # Drop duplicate track_ids
    df = df.drop_duplicates(subset=["track_id"], keep="first")

    # Optional sorting
    if sort_by:
        if sort_by in df.columns:
            df = df.sort_values(sort_by)
        else:
            raise ValueError(f"sort_by={sort_by} not a valid column.")

    # Replace playlist contents
    sp.playlist_replace_items(playlist_id, [])

    # Re-add tracks in correct order
    uris = df["uri"].tolist()
    for i in range(0, len(uris), 100):
        sp.playlist_add_items(playlist_id, uris[i:i+100])

    return df

def df_to_duckdb(con, df, table_name):
    """
    Save a pandas DataFrame to DuckDB table.
    Overwrites existing table.

    Example:
    df_to_duckdb(con, df, "my_table")
    """
    con.register("_temp_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _temp_df")

def duckdb_to_df(con, table_name):
    """
    Load a DuckDB table into a pandas DataFrame.

    Example:
    df = duckdb_to_df(con, "my_table")
    """
    return con.execute(f"SELECT * FROM {table_name}").df()
