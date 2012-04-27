CREATE TABLE artist(
  artistid	INTEGER PRIMARY KEY,
  artistname  	TEXT,
  artistbday	TEXT,
  artistgender	TEXT
);
CREATE TABLE track(
  trackid	INTEGER PRIMARY KEY,
  trackname   	TEXT,
  trackdate	INTEGER,
  tracklength	INTEGER
);
CREATE TABLE album(
  albumid	INTEGER PRIMARY KEY,
  albumname  	TEXT,
  albumdate	INTEGER,
  albumlabel	TEXT
);
CREATE TABLE album_tracks(
  albumid 	INTEGER,
  trackid    	INTEGER,
  PRIMARY KEY(albumid, trackid),
  FOREIGN KEY(albumid) REFERENCES album(albumid),
  FOREIGN KEY(trackid) REFERENCES track(trackid)
);
CREATE TABLE track_artist(
  trackid	INTEGER,
  artistid    	INTEGER,
  FOREIGN KEY(trackid) REFERENCES track(trackid),
  FOREIGN KEY(artistid) REFERENCES artist(artistid)
);
CREATE TABLE album_tracks_artists(
  albumid 	INTEGER,
  trackid    	INTEGER,
  artistid    	INTEGER,
  PRIMARY KEY(albumid, trackid, artistid),
  FOREIGN KEY(albumid) REFERENCES album(albumid),
  FOREIGN KEY(trackid) REFERENCES track(trackid),
  FOREIGN KEY(artistid) REFERENCES artist(artistid)
);
CREATE VIEW artist_discography AS 
select artistname, trackname, albumname
from artist, track, album, track_artist, album_tracks
where artist.artistid = track_artist.artistid 
and track.trackid = track_artist.trackid
and album.albumid = album_tracks.albumid
and album_tracks.trackid = track_artist.trackid;
