create warehouse NEW_WH;

use warehouse NEW_WH;

--create database
create database NEW_DB;

--Create external stage
CREATE OR REPLACE STAGE new_stg
URL='s3://mysnowass4/json/'
CREDENTIALS=(AWS_KEY_ID='AKIA6ODU2AGN5ANARLGL' AWS_SECRET_KEY='hwzpGpCW4/dcDZ+rXz3Ffrdlu9cmKDXSn+q0fLp+');


--CREATE tables in Snowflake
CREATE OR REPLACE TABLE list_table (
    username VARCHAR,
    is_live BOOLEAN
);

create or replace table info_table (
        id STRING,
        url STRING,
        username STRING,
        player_id STRING,
        title STRING,
        ip_address STRING,
        account_id INTEGER,
        status STRING,
        name STRING,
        avatar STRING,
        location STRING,
        country STRING,
        joined DATE,
        last_online DATE,
        followers INTEGER,
        is_streamer BOOLEAN,
        twitch_url STRING,
        fide INTEGER
);


CREATE OR REPLACE TABLE stats_table (
    last_blitz NUMERIC,
    draw_blitz NUMERIC,
    loss_blitz NUMERIC,
    win_blitz NUMERIC,
    last_bullet NUMERIC,
    draw_bullet NUMERIC,
    loss_bullet NUMERIC,
    win_bullet NUMERIC,
    last_rapid NUMERIC,
    draw_rapid NUMERIC,
    loss_rapid NUMERIC,
    win_rapid NUMERIC,
    FIDE NUMERIC,
    primary_key NUMERIC,
    PRIMARY KEY (primary_key)
);

--Create file format
create or replace file format list_json_format
type = json
STRIP_OUTER_ARRAY = TRUE;

create or replace file format info_json_format
type = json
STRIP_OUTER_ARRAY = TRUE;

create or replace file format stats_json_format
type = json
STRIP_OUTER_ARRAY = TRUE;





--create pipe 
CREATE OR REPLACE PIPE list_pipe AUTO_INGEST = TRUE AS
COPY INTO list_table(username, is_live)
FROM (
  SELECT 
    $1:username::VARCHAR AS username,
    $1:is_live::BOOLEAN AS is_live
  FROM @NEW_DB.PUBLIC.new_stg/list_file.json)
  FILE_FORMAT = (FORMAT_NAME = 'NEW_DB.PUBLIC.list_json_format')
  ON_ERROR = 'CONTINUE';

  desc pipe list_pipe;

CREATE OR REPLACE PIPE info_pipe AUTO_INGEST = TRUE AS
COPY INTO info_table(id,url,username,player_id,title,ip_address,account_id,status,name, avatar,location,country,joined,last_online,followers,is_streamer,twitch_url,fide)
FROM (
    SELECT
        $1:id::STRING,
        $1:url::STRING,
        $1:username::STRING,
        $1:player_id::STRING,
        $1:title::STRING,
        $1:ip_address::STRING,
        $1:account_id::INTEGER,
        $1:status::STRING,
        $1:name::STRING,
        $1:avatar::STRING,
        $1:location::STRING,
        $1:country::STRING,
        $1:joined::DATE,
        $1:last_online::DATE,
        $1:followers::INTEGER,
        $1:is_streamer::BOOLEAN,
        $1:twitch_url::STRING,
        $1:fide::INTEGER,
    FROM@NEW_DB.PUBLIC.new_stg/Info_file.json)
    FILE_FORMAT = (FORMAT_NAME = 'NEW_DB.PUBLIC.info_json_format')
    ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE stats_pipe AUTO_INGEST = TRUE AS
COPY INTO stats_table(last_blitz,draw_blitz,loss_blitz,win_blitz,last_bullet,draw_bullet,
loss_bullet,win_bullet,last_rapid,draw_rapid,loss_rapid,win_rapid,FIDE,primary_key)
FROM (
    SELECT
        $1:last_blitz::NUMERIC,
        $1:draw_blitz::NUMERIC,
        $1:loss_blitz::NUMERIC,
        $1:win_blitz::NUMERIC,
        $1:last_bullet::NUMERIC,
        $1:draw_bullet::NUMERIC,
        $1:loss_bullet::NUMERIC,
        $1:win_bullet::NUMERIC,
        $1:last_rapid::NUMERIC,
        $1:draw_rapid::NUMERIC,
        $1:loss_rapid::NUMERIC,
        $1:win_rapid::NUMERIC,
        $1:FIDE::NUMERIC,
        $1:primary_key::NUMERIC
    FROM@NEW_DB.PUBLIC.new_stg/stats_table.json)
    FILE_FORMAT = (FORMAT_NAME = 'NEW_DB.PUBLIC.stats_json_format')
    ON_ERROR = 'CONTINUE';


truncate table list_table;

truncate table info_table;

truncate table stats_table; 

show pipes;

alter pipe list_pipe refresh;
alter pipe info_pipe refresh;
alter pipe stats_pipe refresh;


select * from list_table;
select * from info_table;
select * from stats_table;
