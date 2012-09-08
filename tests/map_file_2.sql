-- "for each grammy there is a unique ID, winner, class, date"
CREATE TABLE grammy(
  grammyid	INTEGER PRIMARY KEY,
  grammywinner	INTEGER NOT NULL REFERENCES album(albumid),
  grammyclass	VARCHAR(16) NOT NULL DEFAULT 'none'
  CHECK (grammyclass IN ('none', 'shmaltz', 'metal', 'exotica')),
  grammydate	DATE
);

CREATE TABLE ouevre(
  artistid	INTEGER NOT NULL REFERENCES artist,
  artistname  	VARCHAR(255) NOT NULL DEFAULT 'none',
  artistgender	VARCHAR(6) NOT NULL DEFAULT 'none'
  CHECK (artistgender IN ('none', 'M', 'F', 'TX')),
  artistbday	DATE,
  trackid	INTEGER NOT NULL DEFAULT 0,
  trackname   	VARCHAR(255) NOT NULL DEFAULT 'none',
  tracklength	INTEGER NOT NULL DEFAULT 0,
  trackdate	DATE,
  albumid	INTEGER NOT NULL DEFAULT 0,
  albumname 	VARCHAR(255) NOT NULL DEFAULT 'none',
  albumdate	DATE,
  labelid	INTEGER NOT NULL DEFAULT 0,
  labelname	VARCHAR(255) NOT NULL DEFAULT 'none',
  labelcity	VARCHAR(255) NOT NULL DEFAULT 'none'
);

--
-- SQL view relvars
--
-- "find all tracks on any album that any artist appears on"
CREATE VIEW artist_discography AS
select artistname, ouevre.artistid as artist_id,
trackname, ouevre.trackid as track_id,
albumname, ouevre.albumid as album_id
from ouevre;


-- for each grammy there is a unique ID, class, date... many-to-one, grammys:album
insert into grammy values (1, 1, 'shmaltz', '2010-04-15');
insert into grammy values (2, 1, 'exotica', '2010-04-15');
insert into grammy values (3, 2, 'metal', '2010-04-15');
insert into grammy values (4, 3, 'shmaltz', '2011-04-15');

-- one big fucked up table, otherwise
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 1, 'love song one', 360, '1999-03-19', 1, 'Blended Up in Black', '2008-08-08', 1, 'Arista', 'Los Angeles');
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 2, 'love song two', 142, '1999-03-19', 1, 'Blended Up in Black', '2008-08-08', 1, 'Arista', 'Los Angeles');
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 2, 'love song two', 142, '2008-08-08', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 3, 'love song three', 220, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 4, 'love song four', 420, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 4, 'love song four', 420, '2008-08-08', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit'); 
insert into ouevre values (1, 'bobby', 'M', '1961-05-15', 5, 'love song five', 361, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 1, 'love song one', 360, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 2, 'love song two', 142, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 2, 'love song two', 142, '2008-08-08', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 3, 'love song three', 220, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 4, 'love song four', 420, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 4, 'love song four', 420, '2008-08-08', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (2, 'diane', 'F', '1960-07-08', 5, 'love song five', 361, '1999-03-19', 1, 'Blended Up in Black', '1999-03-19', 1, 'Arista', 'Los Angeles');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 6, 'hate song one', 180, '2001-08-09', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 6, 'hate song one', 180, '2001-08-09', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 7, 'hate song two', 270, '2001-08-10', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 8, 'hate song three', 145, '2001-08-10', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 8, 'hate song three', 145, '2001-08-10', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 9, 'hate song four', 89, '2001-08-09', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 6, 'hate song one', 180, '2001-08-09', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 6, 'hate song one', 180, '2001-08-09', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 7, 'hate song two', 270, '2001-08-10', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 8, 'hate song three', 145, '2001-08-10', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 8, 'hate song three', 145, '2001-08-10', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 9, 'hate song four', 89, '2001-08-09', 3, 'Dark Night of the Soul', '2009-09-09', 1, 'Arista', 'Los Angeles');
insert into ouevre values (5, 'sid', 'M', '1960-12-24', 10, 'something happened part 1', 135, '1997-08-09', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (5, 'sid', 'M', '1960-12-24', 10, 'something happened part 1', 135, '1997-08-09', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (5, 'sid', 'M', '1960-12-24', 11, 'something happened part 2', 564, '1997-08-10', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (6, 'brian', 'M', '1950-02-25', 10, 'something happened part 1', 135, '1997-08-09', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (6, 'brian', 'M', '1950-02-25', 10, 'something happened part 1', 135, '1997-08-09', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (6, 'brian', 'M', '1950-02-25', 11, 'something happened part 2', 564, '1997-08-10', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (7, 'nancy', 'F', '1950-10-25', 10, 'something happened part 1', 135, '1997-08-09', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (7, 'nancy', 'F', '1950-10-25', 10, 'something happened part 1', 135, '1997-08-09', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (7, 'nancy', 'F', '1950-10-25', 11, 'something happened part 2', 564, '1997-08-10', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 12, 'nothing happened part 1', 876, '1997-08-10', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 12, 'nothing happened part 1', 876, '1997-08-10', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (3, 'lashana', 'TX', '1991-04-09', 13, 'nothing happened part 2', 666, '1997-08-09', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 12, 'nothing happened part 1', 876, '1997-08-10', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 12, 'nothing happened part 1', 876, '1997-08-10', 2, 'Greatest Hits', '2011-05-05', 2, 'Motown', 'Detroit');
insert into ouevre values (4, 'lucy', 'F', '1950-12-25', 13, 'nothing happened part 2', 666, '1997-08-09', 4, 'Songs My Dog Showed Me', '2001-02-01', 3, 'Stax', 'Memphis');
