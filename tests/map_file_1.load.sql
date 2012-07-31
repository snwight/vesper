--
-- bulk load
--
-- 'for each known artist there is a unique ID, name, birthdate, and gender'
insert into artist values (1, 'bobby', 'M', '1961-05-15');
insert into artist values (2, 'diane', 'F', '1960-07-08');
insert into artist values (3, 'lashana', 'TX', '1991-04-09');
insert into artist values (4, 'lucy', 'F', '1950-12-25');
insert into artist values (5, 'sid', 'M', '1960-12-24');
insert into artist values (6, 'brian', 'M', '1950-02-25');
insert into artist values (7, 'nancy', 'F', '1950-10-25');

-- 'for each track there is a unique ID, name, date, length'
insert into track values (1, 'love song one', 360, '2008-08-08');
insert into track values (2, 'love song two', 142, '2008-08-08');
insert into track values (3, 'love song three', 220, '2008-08-08');
insert into track values (4, 'love song four', 420, '2008-08-08');
insert into track values (5, 'love song five', 361, '2008-08-08');
insert into track values (6, 'hate song one', 180, '2001-08-09');
insert into track values (7, 'hate song two', 270, '2001-08-10');
insert into track values (8, 'hate song three', 145, '2001-08-10');
insert into track values (9, 'hate song four', 89, '2001-08-09');
insert into track values (10, 'something happened part 1', 135, '1997-08-09');
insert into track values (11, 'something happened part 2', 564, '1997-08-10');
insert into track values (12, 'nothing happened part 1', 876, '1997-08-10');
insert into track values (13, 'nothing happened part 2', 666, '1997-08-09');

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