CREATE SCHEMA IF NOT EXISTS twitter_stream;

SET search_path TO twitter_stream;

CREATE TABLE twitter_raw_data
(
	tweet CHARACTER VARYING(256)
	,created_at TIMESTAMP WITH TIME ZONE
	,timestamp_ms CHARACTER VARYING(256)
	,favorite_count INTEGER
	,is_favorite BOOLEAN
	,filter_level CHARACTER VARYING(256)
	,id_str CHARACTER VARYING(256)
	,lang CHARACTER VARYING(256)
	,retweet_count INTEGER
	,is_retweeted BOOLEAN
	,"source" CHARACTER VARYING(256)
	,is_truncated BOOLEAN
	,user_description CHARACTER VARYING(256)
	,user_favorites_count INTEGER
	,user_followers_count INTEGER
	,user_friends_count INTEGER
	,user_id_str CHARACTER VARYING(256)
	,user_location CHARACTER VARYING(256)
	,user_name CHARACTER VARYING(256)
	,user_profile_image_url CHARACTER VARYING(256)
	,user_screen_name CHARACTER VARYING(256)
	,user_statuses_count INTEGER
	,user_time_zone CHARACTER VARYING(256)
	,hashtags CHARACTER VARYING(256)
)
;
