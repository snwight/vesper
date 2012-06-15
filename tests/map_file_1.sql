--
-- essential data relvars
--
-- "for each known artist there is a unique ID, name, birthdate, and gender"
CREATE TABLE artist(
  artistid	INTEGER PRIMARY KEY,
  artistname  	TEXT,
  artistbday	DATE,
  artistgender	TEXT CHECK (artistgender IN ('M', 'F', 'TX'))
);
-- "for each track there is a unique ID, name, date, length"
CREATE TABLE track(
  trackid	INTEGER PRIMARY KEY,
  trackname   	TEXT,
  trackdate	DATE,
  tracklength	INTEGER
);
-- "for each album there is a unique ID, name, date"
CREATE TABLE album(
  albumid	INTEGER PRIMARY KEY,
  albumname  	TEXT,
  albumdate	DATE
);
-- "for each label there is a unique ID, name, city"
CREATE TABLE label(
  labelid	INTEGER PRIMARY KEY,
  labelname	TEXT,
  labelcity	TEXT
);
-- "for each grammy there is a unique ID, winner, class, date"
CREATE TABLE grammy(
  grammyid	INTEGER PRIMARY KEY,
  grammywinner	INTEGER,
  grammyclass	TEXT CHECK (grammyclass IN ('shmaltz', 'metal', 'exotica')),
  grammydate	DATE,
  FOREIGN KEY(grammywinner) REFERENCES album(albumid) ON DELETE CASCADE
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
select artistname, artist.artistid as artistid,
trackname, track.trackid as track_id,
albumname, album.albumid as album_id
from artist, track, album, track_artist, album_tracks
where artist.artistid = track_artist.artistid 
and track.trackid = track_artist.trackid
and album.albumid = album_tracks.albumid
and album_tracks.trackid = track_artist.trackid;
--
-- bulk load
--
-- 'for each known artist there is a unique ID, name, birthdate, and gender'
insert into artist values (1, 'bobby', '1961-05-15', 'M');
insert into artist values (2, 'diane', '1960-07-08', 'F');
insert into artist values (3, 'lashana', '1991-04-09', 'TX');
insert into artist values (4, 'lucy', '1950-12-25', 'F');
insert into artist values (5, 'sid', '1960-12-24', 'M');
insert into artist values (6, 'brian', '1950-02-25', 'M');
insert into artist values (7, 'nancy', '1950-10-25', 'F');

-- 'for each track there is a unique ID, name, date, length'
insert into track values (1, 'love song one', '2008-08-08', 360);
insert into track values (2, 'love song two', '2008-08-08', 142);
insert into track values (3, 'love song three', '2008-08-08', 220);
insert into track values (4, 'love song four', '2008-08-08', 420);
insert into track values (5, 'love song five', '2008-08-08', 361);
insert into track values (6, 'hate song one', '2001-08-09', 180);
insert into track values (7, 'hate song two', '2001-08-10', 270);
insert into track values (8, 'hate song three', '2001-08-10', 145);
insert into track values (9, 'hate song four', '2001-08-09', 89);
insert into track values (10, 'something happened part 1', '1997-08-09', 135);
insert into track values (11, 'something happened part 2', '1997-08-10', 564);
insert into track values (12, 'nothing happened part 1', '1997-08-10', 876);
insert into track values (13, 'nothing happened part 2', '1997-08-09', 666);

-- 'for each album there is a unique ID, name, date'
insert into album values (1, 'Dark Night of the Soul', '2009-09-09');
insert into album values (2, 'Blended Up in Black', '1999-03-19');
insert into album values (3, 'Songs My Dog Showed Me', '2001-02-01');
insert into album values (4, 'Greatest Hits', '2011-05-05');

-- 'for each label there is a unique ID, name, and city'
insert into label values (1, 'Arista', 'Los Angeles');
insert into label values (2, 'Stax', 'Memphis');
insert into label values (3, 'Motown', 'Detroit');

-- for each grammy there is a unique ID, album, class, date
insert into grammy values (1, 1, 'shmaltz', '2010-04-15');
insert into grammy values (2, 1, 'exotica', '2010-04-15');
insert into grammy values (3, 2, 'metal', '2010-04-15');
insert into grammy values (4, 3, 'shmaltz', '2011-04-15');
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
