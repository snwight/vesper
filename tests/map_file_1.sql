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
CREATE VIEW tracks AS select artistname, trackname FROM artist, track where trackartist = artist.artistid;