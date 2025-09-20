CREATE TABLE IF NOT EXISTS teams(
  team_id INTEGER PRIMARY KEY,
  name TEXT,
  short_name TEXT
);

CREATE TABLE IF NOT EXISTS players(
  player_id INTEGER PRIMARY KEY,
  team_id INTEGER REFERENCES teams(team_id),
  name TEXT,
  position TEXT,
  foot TEXT,
  birthdate DATE
);

CREATE TABLE IF NOT EXISTS matches(
  match_id BIGINT PRIMARY KEY,
  season INTEGER,
  jornada INTEGER,
  date_utc TIMESTAMP,
  home_team_id INTEGER REFERENCES teams(team_id),
  away_team_id INTEGER REFERENCES teams(team_id),
  home_goals INTEGER,
  away_goals INTEGER,
  status TEXT,
  venue TEXT
);

CREATE TABLE IF NOT EXISTS lineups(
  match_id BIGINT REFERENCES matches(match_id),
  player_id INTEGER REFERENCES players(player_id),
  started BOOLEAN,
  minutes INTEGER,
  sub_in_min INTEGER,
  sub_out_min INTEGER,
  PRIMARY KEY (match_id, player_id)
);

CREATE TABLE IF NOT EXISTS events(
  match_id BIGINT,
  minute INTEGER,
  team_id INTEGER,
  player_id INTEGER,
  type TEXT,                 .
  subtype TEXT NOT NULL DEFAULT '',
  xg DOUBLE,
  xa DOUBLE,
  value DOUBLE,
  PRIMARY KEY (match_id, minute, team_id, player_id, type, subtype)
);


CREATE TABLE IF NOT EXISTS odds_spi(
  match_id BIGINT PRIMARY KEY REFERENCES matches(match_id),
  home_win_p DOUBLE,
  draw_p DOUBLE,
  away_win_p DOUBLE
);

CREATE TABLE IF NOT EXISTS ratings(
  match_id BIGINT REFERENCES matches(match_id),
  player_id INTEGER REFERENCES players(player_id),
  source TEXT,     -- 'DAZN','WhoScored','Marca' etc.
  rating DOUBLE,
  tier INTEGER,
  PRIMARY KEY (match_id, player_id, source)
);
