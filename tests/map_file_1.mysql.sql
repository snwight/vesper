--
-- mysql 5.x version of test schema
-- essential data relvars
--
-- "for each known artist there is a unique ID, name, birthdate, and gender"
CREATE TABLE artist(
  artistid	INTEGER KEY,
  artistname  	VARCHAR(255) NOT NULL,
  artistgender	VARCHAR(6) NOT NULL DEFAULT 'none'
  CHECK (artistgender IN ('none', 'M', 'F', 'TX')),
  artistbday	DATE NOT NULL DEFAULT '0000-00-00'
) ENGINE=InnoDB;
-- "for each track there is a unique ID, name, date, length"
CREATE TABLE track(
  trackid	INTEGER KEY,
  trackname   	VARCHAR(255) NOT NULL,
  tracklength	INTEGER NOT NULL DEFAULT 0,
  trackdate	DATE NOT NULL DEFAULT '0000-00-00'
) ENGINE=InnoDB;
-- "for each album there is a unique ID, name, date"
CREATE TABLE album(
  albumid	INTEGER KEY,
  albumname  	VARCHAR(255) NOT NULL,
  albumdate	DATE NOT NULL DEFAULT '0000-00-00'
) ENGINE=InnoDB;
-- "for each label there is a unique ID, name, city"
CREATE TABLE label(
  labelid	INTEGER KEY,
  labelname	VARCHAR(255) NOT NULL,
  labelcity	VARCHAR(255) NOT NULL
) ENGINE=InnoDB;
-- "for each grammy there is a unique ID, winner, class, date"
CREATE TABLE grammy(
  grammyid	INTEGER PRIMARY KEY,
  grammywinner	INTEGER NOT NULL REFERENCES album,
  grammyclass	VARCHAR(16) NOT NULL DEFAULT 'none'
  CHECK (grammyclass IN ('none', 'shmaltz', 'metal', 'exotica')),
  grammydate	DATE NOT NULL DEFAULT '0000-00-00'
) ENGINE=InnoDB;
--
-- functional dependencies
--
-- "for each album there are one or more possibly non-unique tracks"
CREATE TABLE album_tracks(
  albumid	INTEGER REFERENCES album,
  trackid	INTEGER REFERENCES track,
  PRIMARY KEY(albumid, trackid)
) ENGINE=InnoDB;
-- "for each track there are one or more possibly non-unique artists"
CREATE TABLE track_artist(
  trackid	INTEGER REFERENCES track,
  artistid    	INTEGER REFERENCES artist,
  PRIMARY KEY(trackid, artistid)
) ENGINE=InnoDB;
-- "for each album there is a single possibly non-unique record label"
CREATE TABLE album_label(
  albumid	INTEGER KEY REFERENCES album,
  labelid    	INTEGER REFERENCES label
) ENGINE=InnoDB;
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
