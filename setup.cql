use Killrvideo;

CREATE TABLE users (
	username varchar primary key,
	firstname varchar,
	lastname varchar,
	password varchar
);

CREATE TABLE videos (
	videoid uuid primary key,
	videoname varchar,
	username varchar,
	description varchar,
	tags varchar
);

CREATE INDEX ix_video_users ON videos (username);

CREATE TABLE comments (
	videoid uuid PRIMARY KEY,
	comment varchar
);

CREATE TABLE video_rating (
	videoid uuid PRIMARY KEY,
	rating_counter counter,
	rating_total counter
);

CREATE TABLE video_event (
	videoid uuid,
	username varchar,
	event_type varchar,
	event_date varchar,
	extra_info varchar,
	PRIMARY KEY (videoid, username, event_type)
);

CREATE TABLE tag_index (
	tag varchar,
	videoid uuid,
	info varchar,
	PRIMARY KEY (tag, videoid)
);
	