CREATE TABLE artist(
  artistid	INTEGER PRIMARY KEY,
  artistname  	TEXT
);
CREATE TABLE track(
  trackid	INTEGER PRIMARY KEY,
  trackname   	TEXT
);
CREATE TABLE album(
  albumid	INTEGER PRIMARY KEY,
  albumname  	TEXT
);
CREATE TABLE track_artist(
  artistid    	INTEGER,
  trackid	INTEGER,
  FOREIGN KEY(artistid) REFERENCES artist(artistid),
  FOREIGN KEY(trackid) REFERENCES track(trackid)
);
CREATE TABLE album_tracks(
  albumid 	INTEGER,
  trackid    	INTEGER,
  FOREIGN KEY(albumid) REFERENCES album(albumid),
  FOREIGN KEY(trackid) REFERENCES track(trackid)
);
CREATE VIEW artist_discography AS 
select artistname, trackname, albumname
from artist, track, album, track_artist, album_tracks
where artist.artistid = track_artist.artistid 
and track.trackid = track_artist.trackid
and album.albumid = album_tracks.albumid
and album_tracks.trackid = track_artist.trackid;
