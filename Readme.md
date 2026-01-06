# SpotifyDB

A lightweight toolkit for pulling tracks from Spotify and shaping them into tidy tables for analysis or storage in DuckDB. The utilities rely on [Spotipy](https://spotipy.readthedocs.io/) and pandas to normalize tracks from playlists, albums, artists, or a user's saved library.

## Features
- Flatten Spotify track objects into analysis-friendly rows (including album and artist metadata).
- Convenience loaders for playlists, albums, artists, single tracks, or the current user's saved tracks.
- Normalized outputs as pandas DataFrames that can be written directly to DuckDB or other databases.

## Requirements
- Python 3.9+
- [Spotipy](https://spotipy.readthedocs.io/)
- pandas
- duckdb (optional, for persisting the resulting tables)
- IPython

Install dependencies with:

```bash
pip install spotipy pandas duckdb IPython
```

## Authentication
Spotipy uses OAuth to talk to the Spotify Web API. Create a Spotify app at <https://developer.spotify.com/dashboard> and export the credentials before running code:

```bash
export SPOTIPY_CLIENT_ID="<your_client_id>"
export SPOTIPY_CLIENT_SECRET="<your_client_secret>"
export SPOTIPY_REDIRECT_URI="http://localhost:8080"
```

Then authenticate with:

```python
import spotipy
from spotipy.oauth2 import SpotifyOAuth

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read"))
```

## Usage
The helper functions live in `functions.py`.

### Load tracks from a playlist
```python
from functions import load_tracks_from_playlist

playlist_id = "37i9dQZF1DXcBWIGoYBM5M"  # Today's Top Hits
tracks_df = load_tracks_from_playlist(sp, playlist_id)
```

### Load tracks from an album
```python
from functions import load_tracks_from_album

album_id = "4aawyAB9vmqN3uQ7FjRGTy"
tracks_df = load_tracks_from_album(sp, album_id)
```

### Load tracks from an artist
```python
from functions import load_tracks_from_artist

artist_id = "4dpARuHxo51G3z768sgnrY"  # Adele
tracks_df = load_tracks_from_artist(sp, artist_id)
```

### Load any Spotify resource by URL or URI
```python
from functions import load_any

tracks_df = load_any(sp, "https://open.spotify.com/playlist/37i9dQZF1DX4JAvHpjipBk")
```

### Load saved (liked) tracks for the current user
```python
from functions import load_my_saved_tracks

saved_df = load_my_saved_tracks(sp)
```

## Persisting to DuckDB
Each loader returns a pandas DataFrame you can persist with DuckDB:

```python
import duckdb

con = duckdb.connect("spotify.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS tracks AS SELECT * FROM df").close()
```

## Notebook
The `main.ipynb` notebook offers an interactive starting point for exploring the loaders and exporting to DuckDB.
