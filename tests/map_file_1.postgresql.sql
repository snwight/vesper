--
-- postgresql 9.14 version
-- essential data relvars
--
-- "for each known artist there is a unique ID, name, birthdate, and gender"
CREATE TABLE artist(
  artistid	INTEGER PRIMARY KEY,
  artistname  	TEXT NOT NULL DEFAULT 'unknown',
  artistgender	TEXT NOT NULL DEFAULT 'none'
  CHECK (artistgender IN ('none', 'M', 'F', 'TX')),
  artistbday	DATE
);
-- "for each track there is a unique ID, name, date, length"
CREATE TABLE track(
  trackid	INTEGER PRIMARY KEY,
  trackname   	TEXT NOT NULL DEFAULT 'unknown',
  tracklength	INTEGER NOT NULL DEFAULT 0,
  trackdate	DATE
);
-- "for each album there is a unique ID, name, date"
CREATE TABLE album(
  albumid	INTEGER PRIMARY KEY,
  albumname  	TEXT NOT NULL DEFAULT 'unknown',
  albumdate	DATE
);
-- "for each label there is a unique ID, name, city"
CREATE TABLE label(
  labelid	INTEGER PRIMARY KEY,
  labelname	TEXT NOT NULL DEFAULT 'unknown',
  labelcity	TEXT NOT NULL DEFAULT 'unknown'
);
-- "for each grammy there is a unique ID, winner, class, date"
CREATE TABLE grammy(
  grammyid	INTEGER PRIMARY KEY REFERENCES album,
  grammywinner	INTEGER NOT NULL DEFAULT 0,
  grammyclass	TEXT NOT NULL DEFAULT 'none'
  CHECK (grammyclass IN ('none', 'shmaltz', 'metal', 'exotica')),
  grammydate	DATE
);
--
-- functional dependencies
--
-- "for each album there are one or more possibly non-unique tracks"
CREATE TABLE album_tracks(
  albumid	INTEGER REFERENCES album,
  trackid	INTEGER REFERENCES track,
  PRIMARY KEY(albumid, trackid)
);
-- "for each track there are one or more possibly non-unique artists"
CREATE TABLE track_artist(
  trackid	INTEGER REFERENCES track,
  artistid    	INTEGER REFERENCES artist,
  PRIMARY KEY(trackid, artistid)
);
-- "for each album there is a single possibly non-unique record label"
CREATE TABLE album_label(
  albumid	INTEGER PRIMARY KEY REFERENCES album,
  labelid    	INTEGER REFERENCES label
);
--
-- SQL view relvars
--
-- "find all tracks on any album that any artist appears on"
CREATE VIEW artist_discography AS
select artistname, artist.artistid as artistid,
trackname, track.trackid as track_id,
albumname, album.albumid as album_id
from artist, track, album, track_artist, album_tracks
where artist.artistid = track_artist.artistid 
and track.trackid = track_artist.trackid
and album.albumid = album_tracks.albumid
and album_tracks.trackid = track_artist.trackid;
