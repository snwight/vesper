CREATE TABLE artist(
  artistid    INTEGER PRIMARY KEY, 
  artistname  TEXT
);
CREATE TABLE track(
  trackid     INTEGER PRIMARY KEY,
  trackname   TEXT, 
  trackartist INTEGER,
  FOREIGN KEY(trackartist) REFERENCES artist(artistid)
);
CREATE INDEX trackindex ON track(trackartist);

insert into artist values (1, 'ralph');
insert into artist values (2, 'lauren');
insert into artist values (3, 'diane');