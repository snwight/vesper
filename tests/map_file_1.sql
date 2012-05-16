--
-- essential data relvars
--
-- "for each known artist there is a unique ID, name, birthdate, and gender"
CREATE TABLE artist(
  artistid	INTEGER PRIMARY KEY,
  artistname  	TEXT,
  artistbday	TEXT,
  artistgender	TEXT
);
-- "for each track there is a unique ID, name, date, length"
CREATE TABLE track(
  trackid	INTEGER PRIMARY KEY,
  trackname   	TEXT,
  trackdate	INTEGER,
  tracklength	INTEGER
);
-- "for each album there is a unique ID, name, date, and label ID""
CREATE TABLE album(
  albumid	INTEGER PRIMARY KEY,
  albumname  	TEXT,
  albumdate	INTEGER
);
-- "for each label there is a unique ID, name, and city"
CREATE TABLE label(
  labelid	INTEGER PRIMARY KEY,
  labelname	TEXT,
  labelcity	TEXT
);
--
-- functional dependencies
--
-- "for each album there are one or more possibly non-unique tracks"
CREATE TABLE album_tracks(
  albumid 	INTEGER,
  trackid    	INTEGER,
  PRIMARY KEY(albumid, trackid),
  FOREIGN KEY(albumid) REFERENCES album(albumid) ON DELETE CASCADE,
  FOREIGN KEY(trackid) REFERENCES track(trackid) ON DELETE CASCADE
);
-- "for each track there are one or more possibly non-unique artists"
CREATE TABLE track_artist(
  trackid	INTEGER,
  artistid    	INTEGER,
  PRIMARY KEY(trackid, artistid),
  FOREIGN KEY(trackid) REFERENCES track(trackid) ON DELETE CASCADE,
  FOREIGN KEY(artistid) REFERENCES artist(artistid) ON DELETE CASCADE
);
-- "for each album there is a single possibly non-unique record label"
CREATE TABLE album_label(
  albumid	INTEGER,
  labelid    	INTEGER,
  PRIMARY KEY(albumid),
  FOREIGN KEY(albumid) REFERENCES album(albumid) ON DELETE CASCADE,
  FOREIGN KEY(labelid) REFERENCES label(labelid) ON DELETE CASCADE
);
--
-- SQL view relvars
--
-- "find all tracks on any album that any artist appears on"
CREATE VIEW artist_discography AS
select artistname, trackname, albumname
from artist, track, album, track_artist, album_tracks
where artist.artistid = track_artist.artistid 
and track.trackid = track_artist.trackid
and album.albumid = album_tracks.albumid
and album_tracks.trackid = track_artist.trackid;
--
-- bulk load
--
-- 'for each known artist there is a unique ID, name, birthdate, and gender'
insert into artist values (1, 'bobby', 05151961, 'M');
insert into artist values (2, 'diane', 07081960, 'F');
insert into artist values (3, 'lashana', 04091991, 'TX');
insert into artist values (4, 'lucy', 12251950, 'F');
insert into artist values (5, 'sid', 12241960, 'M');
insert into artist values (6, 'brian', 02251950, 'M');
insert into artist values (7, 'nancy', 10251950, 'F');

-- 'for each track there is a unique ID, name, date, length'
insert into track values (1, 'love song one', 08082008, 360);
insert into track values (2, 'love song two', 08082008, 142);
insert into track values (3, 'love song three', 08082008, 220);
insert into track values (4, 'love song four', 08082008, 420);
insert into track values (5, 'love song five', 08082008, 361);
insert into track values (6, 'hate song one', 08092001, 180);
insert into track values (7, 'hate song two', 08102001, 270);
insert into track values (8, 'hate song three', 08102001, 145);
insert into track values (9, 'hate song four', 08092001, 89);
insert into track values (10, 'something happened part 1', 08091997, 135);
insert into track values (11, 'something happened part 2', 08101997, 564);
insert into track values (12, 'nothing happened part 1', 08101997, 876);
insert into track values (13, 'nothing happened part 2', 08091997, 666);

-- 'for each album there is a unique ID, name, date, and label ID'
insert into album values (1, 'Dark Night of the Soul', 09092009);
insert into album values (2, 'Blended Up in Black', 03191999);
insert into album values (3, 'Songs My Dog Showed Me', 020172001);
insert into album values (4, 'Greatest Hits', 050152012);

-- 'for each label there is a unique ID, name, and city'
insert into label values (1, 'Arista', 'Los Angeles');
insert into label values (2, 'Stax', 'Memphis');
insert into label values (3, 'Motown', 'Detroit');

--
-- functional dependencies
--
-- 'for each album there are one or more possibly non-unique tracks'
insert into album_tracks values (2, 1);
insert into album_tracks values (2, 2);
insert into album_tracks values (2, 3);
insert into album_tracks values (2, 4);
insert into album_tracks values (2, 5);
insert into album_tracks values (1, 6);
insert into album_tracks values (1, 7);
insert into album_tracks values (1, 8);
insert into album_tracks values (1, 9);
insert into album_tracks values (3, 10);
insert into album_tracks values (3, 11);
insert into album_tracks values (3, 12);
insert into album_tracks values (3, 13);
insert into album_tracks values (4, 2);
insert into album_tracks values (4, 4);
insert into album_tracks values (4, 6);
insert into album_tracks values (4, 8);
insert into album_tracks values (4, 10);
insert into album_tracks values (4, 12);

-- 'for each track there are one or more possibly non-unique artists'
insert into track_artist values (1, 1);
insert into track_artist values (2, 1);
insert into track_artist values (3, 1);
insert into track_artist values (4, 1);
insert into track_artist values (5, 1);
insert into track_artist values (1, 2);
insert into track_artist values (2, 2);
insert into track_artist values (3, 2);
insert into track_artist values (4, 2);
insert into track_artist values (5, 2);
insert into track_artist values (6, 3);
insert into track_artist values (7, 3);
insert into track_artist values (8, 3);
insert into track_artist values (9, 3);
insert into track_artist values (6, 4);
insert into track_artist values (7, 4);
insert into track_artist values (8, 4);
insert into track_artist values (9, 4);
insert into track_artist values (10, 5);
insert into track_artist values (11, 5);
insert into track_artist values (10, 6);
insert into track_artist values (11, 6);
insert into track_artist values (10, 7);
insert into track_artist values (11, 7);
insert into track_artist values (12, 4);
insert into track_artist values (13, 4);
insert into track_artist values (12, 3);
insert into track_artist values (13, 3);

-- 'for each album there is a single possibly non-unique record label'
insert into album_label values (1, 1);
insert into album_label values (2, 1);
insert into album_label values (3, 2);
insert into album_label values (4, 3);
