/*
 This is the journey script but all tables renamed so vb is now vbv
 */

drop table if exists central_insights_sandbox.vbv_journey_date_range;
create table central_insights_sandbox.vbv_journey_date_range
(
    min_date varchar(20),
    max_date varchar(20)
);
insert into central_insights_sandbox.vbv_journey_date_range
values ('20210215', '20210221');
SELECT * FROM central_insights_sandbox.vbv_journey_date_range;

-- Step 1: Get consecutive pages for each visit in the date range
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_pages;
CREATE TABLE central_insights_sandbox.vbv_journey_pages AS
SELECT destination,
       dt,
       visit_id,
       hashed_id,
       app_type,
       app_name,
       device_type,
       event_position::INT                                                    as page_position,
       page_name,
       central_insights_sandbox.udf_dataforce_pagename_content_ids(page_name) AS content_id
FROM s3_audience.audience_activity
WHERE destination = 'PS_IPLAYER'
  AND dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
  AND is_signed_in = true
  AND geo_country_site_visited = 'United Kingdom'
  AND ((destination = 'PS_SOUNDS' and source = 'Events') OR
       destination = 'PS_IPLAYER') -- correct source for each destination
  AND NOT (page_name = 'keepalive'
    OR page_name ILIKE '%mvt.activated%'
    OR page_name ILIKE 'iplayer.load.page'
    OR page_name ILIKE 'sounds.startup.page'
    OR page_name ILIKE 'sounds.load.page')
;


SELECT * FROM central_insights_sandbox.vbv_journey_pages LIMIT 10;
-- Step 2: Remove duplicate consecutive pages
-- Step 2a - find previous page
drop table if exists central_insights_sandbox.vbv_journey_deduped_pages;
create table central_insights_sandbox.vbv_journey_deduped_pages as
select destination,
       dt,
       visit_id,
       hashed_id,
       app_type,
       app_name,
       device_type,
       page_position,
       page_name,
       content_id,
       lag(page_name, 1) over (partition by dt, visit_id, destination order by page_position::INT asc) as prev_page
from central_insights_sandbox.vbv_journey_pages
;

-- Step 2b - remove any duplicates
delete
from central_insights_sandbox.vbv_journey_deduped_pages
where page_name = prev_page;

alter table central_insights_sandbox.vbv_journey_deduped_pages
    drop column prev_page;


-- Step 3: Simplify page names to more generic terms
drop table if exists central_insights_sandbox.vbv_journey_page_type;
create table central_insights_sandbox.vbv_journey_page_type as
select destination,
       dt,
       visit_id,
       hashed_id,
       app_type,
       app_name,
       device_type,
       content_id,
       page_position,
       rank() over (partition by dt, destination, visit_id order by page_position::INT asc) as dedeuped_position,
       page_name,
       central_insights_sandbox.udf_dataforce_page_type(page_name)                          AS page_type
from central_insights_sandbox.vbv_journey_deduped_pages
;

-- Only keep the first x de-duped pages per visit - This is to prevent the string getting stupidly long.
delete
from central_insights_sandbox.vbv_journey_page_type
where dedeuped_position > 100;

GRANT ALL on central_insights_sandbox.vbv_journey_page_type to GROUP dataforce_analysts;
GRANT ALL on central_insights_sandbox.vbv_journey_page_type to GROUP central_insights_server;

SELECT * FROM central_insights_sandbox.vbv_journey_page_type LIMIT 10;
----
/*This scripts aims to find all the clicks on to content and link them to the ixpl-start and ixpl-watched flags that send when users view content
There are 3 scripts:    create_master_table_part_1.sql creates the base table.
                        journeys_play_starts_wacthed.sql find the number of valid start/watched flags
                        create_master_table_part_2.sql adds in all the frequency segments, age, gender and start/watch flags
*/

--/////////////////////////////////////////////////////////////////////////////
-- Input tables:  s3_audience.publisher,
--                s3_audience.visits,
--                central_insights_sandbox.journey_date_range
--                central_insights_sandbox.journey_destination
--                central_insights_sandbox.dataforce_journey_page_type
--                prez.scv_vmb

-- Output table: central_insights_sandbox.dataforce_journey_start_watch_complete
--/////////////////////////////////////////////////////////////////////////////

------------ 1.   CREATE a subset of the VMB to use to link content -------------
DROP TABLE IF EXISTS central_insights_sandbox.vbv_vmb_temp;
CREATE TABLE central_insights_sandbox.vbv_vmb_temp AS
SELECT DISTINCT master_brand_name,
                master_brand_id,
                brand_title,
                brand_id,
                series_title,
                series_id,
                episode_id,
                episode_title,
                --programme_duration,
                pips_genre_level_1_names
FROM prez.scv_vmb;


------------ 2. To make life easier with using the publisher table add in unique_visitor_cookie_id into user's table ------------
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_test_users_uv;
CREATE TABLE central_insights_sandbox.vbv_journey_test_users_uv AS
with inlineTemp AS (
    SELECT DISTINCT destination,
                    dt,
                    visit_id,
                    hashed_id,
                    app_type,
                    trunc(date_trunc('week', cast(dt as date))) AS week_commencing
    FROM s3_audience.audience_activity
    WHERE destination = 'PS_IPLAYER'
      AND dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
      AND is_signed_in = true
      AND geo_country_site_visited = 'United Kingdom'

      AND NOT (page_name = 'keepalive'
        OR page_name ILIKE '%mvt.activated%'
        OR page_name ILIKE 'iplayer.load.page'
        OR page_name ILIKE 'sounds.startup.page'
        OR page_name ILIKE 'sounds.load.page'
        )
),
     visits AS (SELECT DISTINCT unique_visitor_cookie_id,
                                visit_id,
                                audience_id,
                                dt
                FROM s3_audience.visits
                WHERE destination = 'PS_IPLAYER' --(SELECT destination FROM central_insights_sandbox.journey_destination)
                  AND dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
     ),
     segment AS (
         SELECT DISTINCT date_of_segmentation,
                         bbc_hid3,
                         age_range,
                         frequency_band,
                         avg_days_between_visits,
                         'iplayer' AS app_name
         FROM iplayer_sandbox.iplayer_weekly_frequency_calculations
         WHERE date_of_segmentation >= (SELECT trunc(date_trunc('week', cast(min_date as date)))
                                        FROM central_insights_sandbox.vbv_journey_date_range)
           AND date_of_segmentation <= (SELECT trunc(date_trunc('week', cast(max_date as date)))
                                        FROM central_insights_sandbox.vbv_journey_date_range)
     )
SELECT DISTINCT a.destination,
                a.dt,
                a.visit_id,
                a.hashed_id,
                a.app_type,
                b.unique_visitor_cookie_id,
                c.age_range,
                case when c.frequency_band is null then 'new' else c.frequency_band end   as frequency_band,
                central_insights_sandbox.udf_dataforce_frequency_groups(c.frequency_band) as frequency_group_aggregated
FROM inlineTemp a
         JOIN visits b ON (a.hashed_id = b.audience_id AND a.visit_id = b.visit_id AND a.dt = b.dt)
         JOIN segment c on a.hashed_id = c.bbc_hid3 AND a.week_commencing = c.date_of_segmentation
WHERE a.destination = 'PS_IPLAYER';
 SELECT * FROM central_insights_sandbox.vbv_journey_test_users_uv LIMIT 10;

------------------------------------------------------------------   Main Process   ---------------------------------------------------------------------------

-------------------------- 3. Select all the different type of content clicks for each visit within our journey table --------------------------


-- Not all tracking is in place properly yet. So not every click will give the ID of the destination content.

-- Clicks can come direct from the homepage, or search, or a channel or category page, or they can come via the TLEO page.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_content_clicks;
CREATE TABLE central_insights_sandbox.vbv_content_clicks AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                a.result,
                a.user_experience,
                a.event_start_datetime
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE (a.attribute LIKE 'content-item%' OR a.attribute LIKE 'start-watching%' OR a.attribute = 'resume' OR
       a.attribute = 'next-episode' OR a.attribute = 'search-result-episode~click' OR a.attribute = 'page-section-related~select')
  AND a.publisher_clicks = 1
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
;


-- Clicks can come from the autoplay system starting an episode
DROP TABLE IF EXISTS central_insights_sandbox.vbv_autoplay_clicks;
CREATE TABLE central_insights_sandbox.vbv_autoplay_clicks AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                CASE
                    WHEN left(right(a.placement, 13), 8) SIMILAR TO '%[0-9]%'
                        THEN left(right(a.placement, 13), 8) -- if this contains a number then its an ep id, if not make blank
                    ELSE 'none' END AS current_ep_id,
                a.result            AS next_ep_id,
                a.user_experience,
                a.event_start_datetime
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE (a.attribute LIKE '%squeeze-auto-play%'
    OR a.attribute LIKE '%squeeze-play%'
    OR a.attribute LIKE '%end-play%'
    OR a.attribute LIKE '%end-auto-play%'
    OR a.attribute LIKE 'auto-play'
    OR a.attribute LIKE 'select-play'
    )
  AND a.publisher_clicks = 1
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;


-- The autoplay on web doesn't currently send any click. It just shows the countdown to autoplay completing as an impression.
-- Include this as a click for now until better tracking is in place
DROP TABLE IF EXISTS central_insights_sandbox.vbv_autoplay_web_complete;
CREATE TABLE central_insights_sandbox.vbv_autoplay_web_complete AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                CASE
                    WHEN left(right(a.placement, 13), 8) SIMILAR TO '%[0-9]%'
                        THEN left(right(a.placement, 13), 8) -- if this contains a number then its an ep id, if not make blank
                    ELSE 'none' END AS current_ep_id,
                a.result            AS next_ep_id,

                a.user_experience,
                a.event_start_datetime
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE ((a.attribute LIKE '%onward-journey-panel~complete%'
  AND a.publisher_impressions = 1) OR (a.attribute LIKE '%onward-journey-panel~select%'
  AND a.publisher_clicks = 1))
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;


-- Deep links into content from off platform. This needs to regex to identify the content pid the link took users too.
-- Not all pids can be identified and not all links go direct to content.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_deeplinks_temp;
CREATE TABLE central_insights_sandbox.vbv_deeplinks_temp AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.url,
                CASE
                    WHEN a.url ILIKE '%/playback%' THEN SUBSTRING(
                            REVERSE(regexp_substr(REVERSE(a.url), '[[:alnum:]]{8}/')), 2,
                            8) -- Need the final instance of the phrase'/playback' to get the episode ID so reverse url so that it's now first.
                    ELSE 'unknown' END                                                                   AS click_result,
                CAST(NULL as varchar ) AS user_experience,
                event_start_datetime,
                row_number()
                over (PARTITION BY a.dt,a.unique_visitor_cookie_id,a.visit_id ORDER BY a.event_position::INT) AS row_count
FROM s3_audience.events a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE a.destination = b.destination
  AND a.url LIKE '%deeplink%'
  AND a.url IS NOT NULL
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;

-- Take only the first deep link instance
-- Later this will be joined to VMB to ensure link takes directly to a content page.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_deeplinks;
CREATE TABLE central_insights_sandbox.vbv_deeplinks AS
SELECT *
FROM central_insights_sandbox.vbv_deeplinks_temp
WHERE row_count = 1;



--- Although clicking 'view-all' does not take the user directly into an episode it takes them to a TLEO which will be given as TLEO = TRUE so these clicks need to be included.
-- The only situation where this click will carry the ID of the content is when it's from an episode page rather than view all on homepage
DROP TABLE IF EXISTS central_insights_sandbox.vbv_view_all_clicks;
CREATE TABLE central_insights_sandbox.vbv_view_all_clicks AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                a.result,
                a.user_experience,
                a.event_start_datetime
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE a.attribute = 'view-all~select'
  AND a.placement ILIKE '%episode%'
  AND a.publisher_clicks = 1
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;

-- Search results often take people to a TLEO page and is commonly used
DROP TABLE IF EXISTS central_insights_sandbox.vbv_search_tleo_clicks;
CREATE TABLE central_insights_sandbox.vbv_search_tleo_clicks AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                a.result,
                a.user_experience,
                a.event_start_datetime
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE a.attribute = 'search-result-TLEO~click'
  AND a.placement ILIKE '%search%'
  AND a.publisher_clicks = 1
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;

------------- Join all the different types of click to content into one table -------------
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_content_clicks;
-- Regular clicks
CREATE TABLE central_insights_sandbox.vbv_journey_content_clicks
AS
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       result AS click_destination_id,
       user_experience,
       event_start_datetime
FROM central_insights_sandbox.vbv_content_clicks;
SELECT * FROM central_insights_sandbox.vbv_journey_content_clicks LIMIT 100;
-- Autoplay
INSERT INTO central_insights_sandbox.vbv_journey_content_clicks
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       next_ep_id AS click_destination_id,
       user_experience,
       event_start_datetime
FROM central_insights_sandbox.vbv_autoplay_clicks;

-- Web autoplay
INSERT INTO central_insights_sandbox.vbv_journey_content_clicks
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       next_ep_id AS click_destination_id,
       user_experience,
       event_start_datetime
FROM central_insights_sandbox.vbv_autoplay_web_complete;

-- Deeplinks
INSERT INTO central_insights_sandbox.vbv_journey_content_clicks
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       CAST('deeplink' AS varchar) AS container,
       CAST('deeplink' AS varchar) AS attribute,
       CAST('deeplink' AS varchar) AS placement,
       click_result                AS click_destination_id,
       user_experience,
       event_start_datetime
FROM central_insights_sandbox.vbv_deeplinks;


-- View all clicks on episodes
INSERT INTO central_insights_sandbox.vbv_journey_content_clicks
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       result AS click_destination_id,
       user_experience,
       event_start_datetime
FROM central_insights_sandbox.vbv_view_all_clicks;

-- Search go to TLEO clicks
INSERT INTO central_insights_sandbox.vbv_journey_content_clicks
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       result AS click_destination_id,
       user_experience,
       event_start_datetime
FROM central_insights_sandbox.vbv_search_tleo_clicks;


--Sometimes there are data issues with duplication, and sometimes a user accidentally clicks back and re-clicks the same content.
--These duplicate click are identified and removed.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_content_clicks_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_content_clicks_temp AS
with get_datediff AS (
    SELECT *,
           row_number()
           over (partition by dt, visit_id, container, attribute, placement, click_destination_id ORDER BY event_position, event_start_datetime) as row_count,
           lag(event_start_datetime)
           over (partition by dt, visit_id, container, attribute, placement, click_destination_id ORDER BY event_position, event_start_datetime) AS datetime_before,
           DATEDIFF(s, datetime_before::DATETIME,
                    event_start_datetime::DATETIME)                                                                                AS datetime_diff
    FROM central_insights_sandbox.vbv_journey_content_clicks
    ORDER BY dt, visit_id, event_position, row_count
)
SELECT *
FROM get_datediff;

-- If a record occurs within 5 seconds of the previous identical record it's removed
-- rename this to be the original table
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_content_clicks;
CREATE TABLE central_insights_sandbox.vbv_journey_content_clicks AS
    SELECT * FROM central_insights_sandbox.vbv_journey_content_clicks_temp
        WHERE datetime_diff >5 OR datetime_diff ISNULL;

-- remove unnecessary columns
ALTER TABLE central_insights_sandbox.vbv_journey_content_clicks DROP COLUMN row_count;
ALTER TABLE central_insights_sandbox.vbv_journey_content_clicks DROP COLUMN datetime_before;
ALTER TABLE central_insights_sandbox.vbv_journey_content_clicks DROP COLUMN datetime_diff;

-- There has been an issue with the user experience field adding the user id to the end
UPDATE central_insights_sandbox.vbv_journey_content_clicks
set user_experience = split_part(user_experience, '?', 1);


--------------------------------------- 4. Select all the iplxp-ep-started impressions -----------------------------------------------------------------

-- For every dt/user/visit combination find all the iplxp-ep-started labels from the user group
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_play_starts;
CREATE TABLE central_insights_sandbox.vbv_journey_play_starts AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                a.result               AS content_id,
                case
                    when metadata like '%PTT=vod%' then 'vod'
                    when metadata like '%PTT=live' then 'live'
                    else 'unknown' end as playback_type,
                case
                    when metadata like '%STT=start%' then 'start'
                    when metadata like '%STT=restart' then 'live-restart'
                    when metadata like '%STT=resume%' then 'vod-resume'
                    else 'unknown' end as start_type,
                event_start_datetime
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b
              ON a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND a.dt = b.dt AND a.visit_id = b.visit_id
WHERE a.publisher_impressions = 1
  AND a.attribute = 'iplxp-ep-started'
  AND a.destination = 'PS_IPLAYER'--(SELECT destination FROM central_insights_sandbox.journey_destination)
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;



--------------------------------------- 5. Join clicks and start flags -----------------------------------------------------------------
-- Join clicks and starts into one master table. (some clicks will not be to a content page i.e homepage > TLEO and will be dealt with later)
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts_temp;
-- Add in start events
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp AS
SELECT *, CAST( NULL as varchar(4000)) AS user_experience
FROM central_insights_sandbox.vbv_journey_play_starts;


-- Add in click events
INSERT INTO central_insights_sandbox.vbv_journey_clicks_and_starts_temp
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       click_destination_id AS content_id,
       CAST( NULL as varchar(4000)) AS playback_type,
       CAST( NULL as varchar(4000)) AS start_type,
       event_start_datetime,
       user_experience
FROM central_insights_sandbox.vbv_journey_content_clicks;


---------  5b. Identify if clicks went to content via a tleo -  START --------

--a. This selects all the clicks to content in order in a visit.
-- it rolls up the click from the row below into the record
-- and selects any records where there is a click (homepage, categories, content page etc) followed by a click on a TLEO page
-- clicks that go via a TLEO page have a series or brand id, not an epiosde id, so any records with an episode id are removed to speed up the process.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_via_tleo_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_via_tleo_temp AS
    -- add in a check to see if the first click is to a brand, series or episode ID
    -- get the information from the row below into the row. This will then be checked to see if it's valid to join them or if they just happened to be neighbouring unrelated rows.
with get_data AS (
    SELECT *,
           CASE
               WHEN content_id in (SELECT DISTINCT brand_id FROM central_insights_sandbox.vbv_vmb_temp)
                   THEN 'brand_id'
               WHEN content_id in (SELECT DISTINCT series_id FROM central_insights_sandbox.vbv_vmb_temp)
                   THEN 'series_id'
               WHEN content_id in (SELECT DISTINCT episode_id FROM central_insights_sandbox.vbv_vmb_temp)
                   THEN 'episode_id'
               WHEN content_id = 'unknown' THEN 'unknown'
               END                                             as first_id_type,
           lead(attribute)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_attribute,
           lead(container)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_container,
           lead(placement)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_placement,
           lead(event_position)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_event_pos,
           lead(content_id)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_episode_id,
           lead(user_experience)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_user_experience,
           lead(event_start_datetime)
           OVER (partition by dt, hashed_id, visit_id order by event_position) AS next_click_event_start_datetime,
           DATEDIFF(s, event_start_datetime::DATETIME,
                    next_click_event_start_datetime::DATETIME)                                                                                AS datetime_diff
    FROM central_insights_sandbox.vbv_journey_clicks_and_starts_temp
    WHERE attribute != 'iplxp-ep-started'
    ORDER BY dt, visit_id, event_position
),
     distinct_ep_ids AS ( --ensure no duplication when there's more than one record for an episode id
         SELECT DISTINCT brand_id, series_id, episode_id FROM prez.scv_vmb
     )
-- get all the pairs that go page X to TLEO (e.g homepage-TLEO) but do not allow TLEO->TLEO
SELECT a.dt,
       hashed_id,
       a.visit_id,
       attribute,
       container,
       placement,
       user_experience,
       next_click_attribute,
       next_click_container,
       next_click_placement,
       next_click_user_experience,
       event_position,
       next_click_event_pos,
       content_id      as first_page_click_id,
       first_id_type,
       next_click_episode_id AS tleo_episode_id,
       b.brand_id AS tleo_brand_id,
       b.series_id AS tleo_series_id,
       event_start_datetime,
       next_click_event_start_datetime,
       datetime_diff
FROM get_data a
LEFT JOIN distinct_ep_ids b on a.next_click_episode_id  = b.episode_id
WHERE next_click_placement ILIKE '%tleo%'
  AND placement NOT ILIKE '%tleo%'
AND first_id_type != 'episode_id'
;


-- b. Now remove any rows that are not valid
-- they're valid when the first click was to the brand or series of the content clicked on the TLEO page
-- or if the first click was unknown (channels/categories/search) but within 60s
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_via_tleo_valid;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_via_tleo_valid AS
    with check_validity AS (
        SELECT DISTINCT *,
               CASE
                   WHEN first_id_type = 'series_id' AND first_page_click_id = tleo_series_id
                       THEN 'valid'
                   WHEN first_id_type = 'brand_id' AND first_page_click_id = tleo_brand_id THEN 'valid'
                   WHEN first_id_type = 'unknown' AND datetime_diff <=60 THEN 'valid'
                   ELSE 'not valid'
                   END as validity_check
        FROM central_insights_sandbox.vbv_journey_clicks_via_tleo_temp
    )
    SELECT * FROM check_validity
WHERE validity_check = 'valid';


-- Add a helper column to be used to delete/edit these rows from the clicks to content table
ALTER TABLE central_insights_sandbox.vbv_journey_clicks_via_tleo_valid
    ADD first_page_identifier varchar(255);
ALTER TABLE central_insights_sandbox.vbv_journey_clicks_via_tleo_valid
    ADD tleo_identifier varchar(255);


UPDATE central_insights_sandbox.vbv_journey_clicks_via_tleo_valid
SET first_page_identifier  = dt||'-'||visit_id||'-'||event_position;
UPDATE central_insights_sandbox.vbv_journey_clicks_via_tleo_valid
SET tleo_identifier = dt||'-'||visit_id||'-'||next_click_event_pos;


-- Alter the table with all the clicks
-- Add helper column to only change the rows required
ALTER TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp
    ADD helper varchar(400);
UPDATE central_insights_sandbox.vbv_journey_clicks_and_starts_temp
SET helper = dt||'-'||visit_id||'-'||event_position ;



-- Update the clicks and starts table. Any page that lead to content via a TLEO is given a tleo = true flag, and the TLEO page removed.
-- Add in another column and label it with TLEO = TRUE if they went via a TLEO
ALTER TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp
ADD via_tleo bool;
-- set the first page record (i.e the homepage record) to say it goes via TLEO
UPDATE central_insights_sandbox.vbv_journey_clicks_and_starts_temp
SET via_tleo = TRUE
WHERE helper in  (SELECT first_page_identifier FROM central_insights_sandbox.vbv_journey_clicks_via_tleo_valid);

-- Also need to add in the TLEO click container and attribute, do this with a join.
DROP TABLE IF EXISTS vbv_add_tleo_info;
CREATE TEMP TABLE vbv_add_tleo_info AS
    SELECT a.*, b.next_click_attribute AS tleo_attribute, b.next_click_container AS tleo_container
    FROM central_insights_sandbox.vbv_journey_clicks_and_starts_temp a
LEFT JOIN central_insights_sandbox.vbv_journey_clicks_via_tleo_valid b on a.helper = b.first_page_identifier;

-- Drop the table and re-create with the new columns
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp AS SELECT * FROM vbv_add_tleo_info;
DROP TABLE IF exists vbv_add_tleo_info;


-- the most accurate content id comes from the TLEO page where it's the episode_id not a brand or series.
-- so change the content id from the initial click (i.e homepage) to that TLEO click id.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts_temp2;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp2 AS
SELECT a.*, b.tleo_episode_id
FROM central_insights_sandbox.vbv_journey_clicks_and_starts_temp a
LEFT JOIN central_insights_sandbox.vbv_journey_clicks_via_tleo_valid b on a.helper = first_page_identifier;

UPDATE central_insights_sandbox.vbv_journey_clicks_and_starts_temp2
set content_id = tleo_episode_id
WHERE tleo_episode_id IS NOT NULL AND via_tleo IS TRUE;

-- make this edited table back to the original name
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp AS
    SELECT * FROM central_insights_sandbox.vbv_journey_clicks_and_starts_temp2;

-- delete the TLEO record
DELETE FROM central_insights_sandbox.vbv_journey_clicks_and_starts_temp
WHERE helper in (SELECT tleo_identifier FROM central_insights_sandbox.vbv_journey_clicks_via_tleo_valid);

ALTER TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp
    DROP COLUMN helper;
ALTER TABLE central_insights_sandbox.vbv_journey_clicks_and_starts_temp
    DROP COLUMN tleo_episode_id;

DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts_temp2;

---------  5b. Identify if clicks went to content via a tleo - FINISH --------

--------------------------------------- 5. Join clicks and start flags - continued -----------------------------------------------------------------

-- Add in row number for each visit
-- This is used to match a content click to a start if the click carried no ID (i.e with categories or channels pages)
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_and_starts AS
SELECT *, row_number() over (PARTITION BY dt,unique_visitor_cookie_id,hashed_id, visit_id ORDER BY event_position::INT)
FROM central_insights_sandbox.vbv_journey_clicks_and_starts_temp
--ORDER BY dt, unique_visitor_cookie_id, hashed_id, visit_id, event_position
;


-- Join the table back on itself to match the content click to the iplxp-ep-started by the content_id.
-- For categories and channels the click ID is often unknown so need to create one master table so the click event before iplxp-ep-started can be taken in these cases
-- If that's ever fixed then can simply join play starts with clicks
-- The clicks and start flags are split into two temp tables for ease of code.
-- Can't just join the two original tables because we need the row count for when the content_id is unknown.
DROP TABLE IF EXISTS vbv_temp_starts;
DROP TABLE IF EXISTS vbv_temp_clicks;
CREATE TABLE central_insights_sandbox.vbv_temp_starts AS SELECT * FROM central_insights_sandbox.vbv_journey_clicks_and_starts WHERE attribute = 'iplxp-ep-started';
CREATE TABLE central_insights_sandbox.vbv_temp_clicks AS SELECT * FROM central_insights_sandbox.vbv_journey_clicks_and_starts WHERE attribute != 'iplxp-ep-started';


DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_linked_starts_temp AS
SELECT a.dt,
       a.unique_visitor_cookie_id,
       a.hashed_id,
       a.visit_id,
       a.event_position                     AS click_event_position,
       a.container                          AS click_container,
       a.attribute                          AS click_attribute,
       a.placement                          AS click_placement,
       a.content_id                         AS click_episode_id,
       a.user_experience                    AS click_user_experience,
       a.via_tleo                           AS via_tleo,
       a.tleo_container                     AS tleo_container,
       a.tleo_attribute                     AS tleo_attribute,
       b.container                          AS content_container,
       ISNULL(b.attribute, 'no-start-flag') AS content_attribute,
       b.placement                          AS content_placement,
       b.playback_type,
       b.start_type,
       b.content_id                         AS content_id,
       b.event_position                     AS content_start_event_position,
       CASE
           WHEN b.event_position IS NOT NULL THEN CAST(b.event_position - a.event_position AS integer)
           ELSE 0 END                       AS content_start_diff
FROM central_insights_sandbox.vbv_temp_clicks a
         LEFT JOIN central_insights_sandbox.vbv_temp_starts b
                   ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                      a.visit_id = b.visit_id AND CASE
                                                      WHEN a.content_id != 'unknown'
                                                          THEN a.content_id = b.content_id -- Check the content IDs match if possible
                                                      WHEN a.content_id = 'unknown'
                                                          THEN a.row_number = b.row_number - 1 -- Click is row above start - if you can't check IDs or master brands, just link with row above (click is one above start)
                          END
WHERE content_start_diff >= 0
-- For the null cases with no matching start flag the value given = 0.
--ORDER BY a.event_position
;

-- Prevent the join over counting
-- Prevent one click being joined to multiple starts
-- duplicate count 1 identifies records which need to be deleted (one click duplicted when it's joined to multiple starts
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_valid_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_linked_starts_valid_temp AS
SELECT *,
       CASE
           WHEN content_attribute = 'iplxp-ep-started' THEN row_number()
                                                            over (PARTITION BY dt,unique_visitor_cookie_id,hashed_id, visit_id,click_event_position ORDER BY content_start_diff)
           ELSE 1 END AS duplicate_count,
       CASE
           WHEN content_attribute = 'iplxp-ep-started' THEN row_number()
                                                            over (PARTITION BY dt,unique_visitor_cookie_id,hashed_id, visit_id,content_start_event_position ORDER BY content_start_diff)
           ELSE 1 END AS duplicate_count2
FROM central_insights_sandbox.vbv_journey_clicks_linked_starts_temp
--ORDER BY dt, hashed_id, visit_id, content_start_event_position
;

-- Update table so duplicate joins have the ixpl-ep-started label set to null.
-- If two clicks are joined to the same start, make null the record for row with the largest content_start_diff as this is an incorrect join.
-- This retains both clicks and just the one start
UPDATE central_insights_sandbox.vbv_journey_clicks_linked_starts_valid_temp
SET content_container = NULL,
    content_attribute = 'no-start-flag',
    content_placement = NULL,
    content_id = NULL,
    content_start_event_position = NULL,
    content_start_diff = NULL,
    playback_type = NULL,
    start_type = NULL
WHERE duplicate_count2 != 1;

-- Remove records where a click has been accidentally duplicated.
DELETE FROM central_insights_sandbox.vbv_journey_clicks_linked_starts_valid_temp
WHERE duplicate_count != 1;

-- The clicks and starts are now validated
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_valid;
CREATE TABLE central_insights_sandbox.vbv_journey_clicks_linked_starts_valid
AS SELECT * FROM central_insights_sandbox.vbv_journey_clicks_linked_starts_valid_temp;

-- Define value if there's no start
UPDATE central_insights_sandbox.vbv_journey_clicks_linked_starts_valid
SET content_attribute = (CASE
                             WHEN content_attribute IS NULL THEN 'no-start-flag'
                             ELSE content_attribute END);


------------------------------------------------------ 6. Simplify/clean up table --------------------------------------------------------------------------------------------------------

/*-- Simplify the page names.
UPDATE central_insights_sandbox.vbv_journey_clicks_linked_starts_valid
SET click_placement = central_insights_sandbox.udf_dataforce_page_type(click_placement);

UPDATE central_insights_sandbox.vbv_journey_clicks_linked_starts_valid
SET content_placement = central_insights_sandbox.udf_dataforce_page_type(content_placement);
*/

-- Select only useful columns
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_starts;
CREATE TABLE central_insights_sandbox.vbv_journey_valid_starts AS
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       click_attribute,
       click_container,
       click_placement,
       click_user_experience,
       via_tleo,
       tleo_container,
       tleo_attribute,
       click_episode_id,
       click_event_position,
       content_attribute,
       content_placement,
       content_id,
       playback_type,
       start_type,
       content_start_event_position
FROM central_insights_sandbox.vbv_journey_clicks_linked_starts_valid;


-- The user may click to a tleo or brand page (i.e not an episode page) where there is no option of a play start.
-- because this may lead to a TLEO step that has been accounted for, these clicks are valid and should stay in the table


------------------------------------------------------------- 7. Add in play watched flags ----------------------------------------------------------------------------------------
-- For every dt/user/visit combination find all the ixpl watched labels
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_play_watched;
CREATE TABLE central_insights_sandbox.vbv_journey_play_watched AS
SELECT DISTINCT a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                a.result AS content_id,
                a.user_experience
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b
              ON a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND a.dt = b.dt AND a.visit_id = b.visit_id
WHERE a.publisher_impressions = 1
  AND a.attribute = 'iplxp-ep-watched'
  AND a.destination = 'PS_IPLAYER' --(SELECT destination FROM central_insights_sandbox.journey_destination)
  AND a.dt BETWEEN (SELECT min_date FROM central_insights_sandbox.vbv_journey_date_range) AND (SELECT max_date FROM central_insights_sandbox.vbv_journey_date_range)
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.event_position
;



-- Join the watch events to the validated start events, ensuring the same content_id
-- Create duplicate flags to ensure that one start is not joined to multiple watch flags and vice versa.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_starts_and_watched;
CREATE TABLE central_insights_sandbox.vbv_journey_starts_and_watched AS
SELECT a.*,
       ISNULL(b.attribute, 'no-watched-flag') AS watched_flag,
       b.event_position                       AS content_watched_event_position,
       b.content_id                           AS watched_content_id,
       CASE
           WHEN b.event_position Is NOT NULL THEN CAST(b.event_position - a.content_start_event_position AS integer)
           ELSE 0 END                         AS start_watched_diff,
       CASE
           WHEN watched_flag = 'iplxp-ep-watched' THEN row_number()
                                                             over (PARTITION BY a.dt,a.unique_visitor_cookie_id,a.hashed_id, a.visit_id, a.content_start_event_position  ORDER BY start_watched_diff)
           ELSE 1 END                         AS duplicate_count,
       CASE
           WHEN content_attribute = 'iplxp-ep-started' AND watched_flag = 'iplxp-ep-watched' THEN
                       row_number() over (partition by a.dt,a.unique_visitor_cookie_id,a.hashed_id, a.visit_id, content_watched_event_position ORDER BY (start_watched_diff))
           ELSE 1 END AS duplicate_count2
FROM central_insights_sandbox.vbv_journey_valid_starts a
         LEFT JOIN central_insights_sandbox.vbv_journey_play_watched b
                   ON a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND a.dt = b.dt AND
                      a.visit_id = b.visit_id AND a.content_id = b.content_id
WHERE start_watched_diff >= 0
--ORDER BY a.dt, b.hashed_id, a.visit_id, a.click_event_position
;

-- Set values to null where a watched event has been incorrectly joined to a second start.
UPDATE central_insights_sandbox.vbv_journey_starts_and_watched
SET watched_content_id = NULL,
    content_watched_event_position = NULL,
    start_watched_diff = NULL,
    watched_flag = 'no-watched_flag'
WHERE duplicate_count2 != 1;

-- remove records accidentally duplicated
DELETE FROM central_insights_sandbox.vbv_journey_starts_and_watched
WHERE duplicate_count != 1;



-- Simplify table columns
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_watched;
CREATE TABLE central_insights_sandbox.vbv_journey_valid_watched AS
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       click_episode_id,
       click_event_position,
       click_attribute,
       click_container,
       click_placement,
       click_user_experience,
       via_tleo,
       tleo_container,
       tleo_attribute,
       content_placement,
       content_id,
       playback_type,
       start_type,
       content_start_event_position,
       content_watched_event_position,
       content_attribute  AS start_flag,
       watched_flag
FROM central_insights_sandbox.vbv_journey_starts_and_watched;

--In case any null values have slipped through
UPDATE central_insights_sandbox.vbv_journey_valid_watched
SET start_flag = (CASE
                      WHEN start_flag IS NULL THEN 'no-start-flag'
                      ELSE start_flag END);
UPDATE central_insights_sandbox.vbv_journey_valid_watched
SET watched_flag = (CASE
                        WHEN watched_flag IS NULL THEN 'no-watched-flag'
                        ELSE watched_flag END);


------------------------------------------------------------------8.  Orphan starts ---------------------------------------------------------------------------------------------------
-- There are play starts and watch events that appear to have no click to content
-- One reason is that user's load a new session directly onto a content page i.e saved link on responsive web

--- Orphaned starts -- i.e those with no click to content
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_starts;
CREATE TABLE central_insights_sandbox.vbv_journey_orphan_starts AS
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       content_id,
       playback_type,
       start_type
FROM (
         SELECT a.*, b.visit_id as missing_flag
         FROM central_insights_sandbox.vbv_journey_play_starts a
                  LEFT JOIN central_insights_sandbox.vbv_journey_valid_starts b
                            ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                               a.visit_id = b.visit_id AND a.event_position = b.content_start_event_position)
WHERE missing_flag IS NULL;

-- Orphan watched events
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_watched;
CREATE TABLE central_insights_sandbox.vbv_journey_orphan_watched AS
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       content_id,
       CAST( NULL as varchar(4000)) AS playback_type,
       CAST( NULL as varchar(4000)) AS start_type

FROM (
         SELECT a.*, b.visit_id as missing_flag
         FROM central_insights_sandbox.vbv_journey_play_watched a
                  LEFT JOIN central_insights_sandbox.vbv_journey_valid_watched b
                            ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                               a.visit_id = b.visit_id AND a.event_position = b.content_watched_event_position)
WHERE missing_flag IS NULL;


-- Link the orphaned watched flags to their corresponding starts.
-- Identify starts that are joined to multiple watch flags or the reverse and turn the values null or remove them.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp AS
SELECT a.dt,
       a.unique_visitor_cookie_id,
       a.hashed_id,
       a.visit_id,
       a.event_position as start_event_position,
       a.container,
       a.attribute      as start_flag,
       a.placement,
       a.playback_type,
       a.start_type,
       a.content_id     AS start_id,
       b.content_id     AS watched_id,
       b.event_position as watched_event_position,
       b.attribute      as watched_flag,
       CASE
           WHEN b.event_position Is NOT NULL THEN CAST(b.event_position - a.event_position AS integer)
           ELSE 0 END   AS start_watched_diff,
       CASE
           WHEN b.attribute = 'iplxp-ep-watched' THEN row_number()
                                                      over (PARTITION BY a.dt,a.unique_visitor_cookie_id,a.hashed_id, a.visit_id,start_event_position ORDER BY start_watched_diff)
           ELSE 1 END   AS duplicate_count,
        CASE
           WHEN b.attribute = 'iplxp-ep-watched' THEN row_number()
                                                      over (PARTITION BY a.dt,a.unique_visitor_cookie_id,a.hashed_id, a.visit_id,watched_event_position ORDER BY start_watched_diff)
           ELSE 1 END   AS duplicate_count2
FROM central_insights_sandbox.vbv_journey_orphan_starts a
         LEFT JOIN central_insights_sandbox.vbv_journey_orphan_watched b
                   ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                      a.visit_id = b.visit_id AND a.content_id = b.content_id
WHERE start_watched_diff >= 0;


-- Set values to null where a watched event has been incorrectly joined to a second start.
UPDATE central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp
SET watched_id = NULL,
    watched_event_position = NULL,
    start_watched_diff = NULL,
    watched_flag = 'no-watched_flag'
WHERE duplicate_count2 != 1;

-- remove records accidentally duplicated
DELETE FROM central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp
WHERE duplicate_count != 1;



DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched;
CREATE TABLE central_insights_sandbox.vbv_journey_orphan_start_watched AS
SELECT dt,
       unique_visitor_cookie_id,
       hashed_id,
       visit_id,
       start_event_position,
       container,
       start_flag,
       placement,
       playback_type,
       start_type,
       start_id,
       watched_id,
       watched_event_position,
       watched_flag
FROM central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp;

-- make sure nothing comes through null
UPDATE central_insights_sandbox.vbv_journey_orphan_start_watched
SET watched_flag = (CASE
                        WHEN watched_flag IS NULL THEN 'no-watched-flag'
                        ELSE watched_flag END);


--- Simplify names
/*UPDATE central_insights_sandbox.vbv_journey_orphan_start_watched
SET placement = central_insights_sandbox.udf_dataforce_page_type(placement);*/

-- Create a table that has the same fields as the master start and watched table so the orphan events can be inserted
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_orphan_start_watched_temp AS
    SELECT dt,
           unique_visitor_cookie_id,
           hashed_id,
           visit_id,
           CAST('unknown' AS varchar ) AS click_episode_id,
           CAST(NULL AS integer ) AS click_event_position,
           CAST('unknown' AS varchar ) AS click_attribute,
            CAST('unknown' AS varchar ) AS click_container,
            CAST('unknown' AS varchar ) AS click_placement,
           CAST(NULL as varchar) AS click_user_experience,
           CAST(FALSE AS boolean) AS via_tleo,
           CAST(NULL as varchar) AS tleo_container,
           CAST(NULL as varchar) AS tleo_attribute,
           placement AS content_placement,
           start_id AS content_id,
           playback_type,
           start_type,
           start_event_position AS content_start_event_position,
           watched_event_position AS content_watched_event_position,
           start_flag,
           watched_flag
FROM central_insights_sandbox.vbv_journey_orphan_start_watched;


--- Add orphans into into master table
INSERT INTO central_insights_sandbox.vbv_journey_valid_watched
SELECT * FROM central_insights_sandbox.vbv_journey_orphan_start_watched_temp;



-------------------------- 9. Defining the order of episodes and relating to the master table ----------------------
-- Ensure episodes are in the correct order
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_final_starts_watched_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_final_starts_watched_temp AS
SELECT b.destination,
       a.dt,
       a.unique_visitor_cookie_id,
       a.hashed_id,
       b.app_type,
       a.visit_id,
       CASE
           WHEN click_event_position ISNULL THEN content_start_event_position
           WHEN click_event_position IS NOT NULL THEN click_event_position END AS ep_order,
       a.click_attribute,
       a.click_container,
       a.click_placement,
       a.click_user_experience,
       a.via_tleo,
       a.tleo_container,
       a.tleo_attribute,
       a.content_placement,
       CASE
           WHEN a.content_id ISNULL THEN a.click_episode_id
           ELSE a.content_id END                                               AS content_id,
       playback_type,
       start_type,
       a.start_flag,
       a.watched_flag
FROM central_insights_sandbox.vbv_journey_valid_watched a
         LEFT JOIN central_insights_sandbox.vbv_journey_test_users_uv b
                   ON a.dt = b.dt AND a.hashed_id = b.hashed_id AND a.visit_id = b.visit_id

;

-------------------------------------------------------------------------------------------------------------------------------------
-- A journey could begin on an episode page, i.e a link someone's saved or their visit timed out and they re-activated it
-- These would register no click to content but could have a start or watched event.
-- Start/watched would be included with the orphan events but for later work we need the number of episode pages identified to match the master table.
-- This is what the next part does
--------------------------------------------------------------------------------------------------------------------------------------

-- Find all the distinct episode pages for a visit
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_first_episode_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_first_episode_temp AS
SELECT DISTINCT a.destination,
                a.dt,
                a.unique_visitor_cookie_id,
                b.hashed_id,
                b.app_type,
                a.visit_id,
                a.event_position,
                a.container,
                a.attribute,
                a.placement,
                case
                    when metadata like '%PTT=vod%' then 'vod'
                    when metadata like '%PTT=live' then 'live'
                    else 'unknown' end as playback_type,
                case
                    when metadata like '%STT=start%' then 'start'
                    when metadata like '%STT=restart' then 'live-restart'
                    when metadata like '%STT=resume%' then 'vod-resume'
                    else 'unknown' end as start_type
FROM s3_audience.publisher a
         JOIN central_insights_sandbox.vbv_journey_test_users_uv b -- this is to bring in only those visits in our journey table
              ON a.dt = b.dt AND a.unique_visitor_cookie_id = b.unique_visitor_cookie_id AND
                 b.visit_id = a.visit_id
WHERE a.placement ILIKE '%episode%'
  AND a.destination = b.destination
  AND a.dt BETWEEN (SELECT min(dt) FROM central_insights_sandbox.vbv_journey_test_users_uv) AND (SELECT min(dt)
                                                                                                FROM central_insights_sandbox.vbv_journey_test_users_uv)
;

AlTER TABLE central_insights_sandbox.vbv_journey_first_episode_temp
ADD current_ep_id varchar(400);

-- Get the content ID from the placement field
UPDATE central_insights_sandbox.vbv_journey_first_episode_temp
SET current_ep_id = (CASE
                         WHEN left(right(placement, 13), 8) SIMILAR TO '%[0-9]%'
                             THEN left(right(placement, 13), 8) -- if this contains a number then its an ep id, if not make blank
                         ELSE 'none'
    END);

-- Find which of those are distinct
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_distinct_episodes;
CREATE TABLE central_insights_sandbox.vbv_journey_distinct_episodes AS
SELECT destination,
       dt,
       unique_visitor_cookie_id,
       hashed_id,
       app_type,
       visit_id,
       event_position,
       container,
       attribute,
       placement,
       current_ep_id,
       playback_type,
       start_type
FROM (SELECT *,
             row_number()
             over (PARTITION BY dt,unique_visitor_cookie_id, hashed_id, visit_id, placement ORDER BY event_position::INT) AS row_count
      FROM central_insights_sandbox.vbv_journey_first_episode_temp
      ORDER BY dt, hashed_id, visit_id, event_position)
WHERE row_count = 1;


-- put these into the main table
ALTER TABLE central_insights_sandbox.vbv_journey_final_starts_watched_temp
ADD COLUMN added_page varchar;

-- Select any pages that come before the first episode already identified and add them into the table with start/watched flags as blank
INSERT INTO central_insights_sandbox.vbv_journey_final_starts_watched_temp
SELECT a.destination,
       a.dt,
       a.unique_visitor_cookie_id,
       a.hashed_id,
       a.app_type,
       a.visit_id,
       event_position                     AS ep_order,
       CAST('unknown' AS varchar)         AS click_attribute,
       CAST('unknown' AS varchar)         AS click_container,
       CAST('unknown' AS varchar)         AS click_placement,
       CAST(NULL as varchar)              AS click_user_experience,
       CAST(FALSE as boolean)             AS via_tleo,
       CAST(NULL as varchar)              AS tleo_container,
       CAST(NULL as varchar)              AS tleo_attribute,
       CAST('episode_page' AS varchar)    AS content_placement,
       a.current_ep_id                    AS content_id,
       playback_type,
       start_type,
       CAST('no-start-flag' AS varchar)   as start_flag,
       CAST('no-watched-flag' AS varchar) as watched_flag,
       CAST('added-page' AS varchar)      AS added_page
FROM central_insights_sandbox.vbv_journey_distinct_episodes a
         LEFT JOIN (SELECT dt,
                           unique_visitor_cookie_id,
                           hashed_id,
                           app_type,
                           visit_id,
                           min(ep_order) AS min_ep_order
                    FROM central_insights_sandbox.vbv_journey_final_starts_watched_temp
                    GROUP BY dt, unique_visitor_cookie_id, hashed_id, app_type, visit_id) b
                   ON a.dt = b.dt AND a.hashed_id = b.hashed_id AND a.visit_id = b.visit_id
WHERE a.attribute != 'iplxp-ep-watched'
  AND (a.event_position < b.min_ep_order OR b.min_ep_order ISNULL);
-- We don't want any watched flags with no start, this would just be confusing.



-- Check if the added rows come directly before a row with the same ID
-- If so they're a duplciate and need removing, if not keep
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_final_starts_watched_temp2;
CREATE TABLE central_insights_sandbox.vbv_journey_final_starts_watched_temp2 AS
SELECT *,
       lead(content_id, 1)
       OVER (PARTITION BY dt, visit_id, hashed_id, app_type, visit_id ORDER BY ep_order) as duplicate_check
FROM central_insights_sandbox.vbv_journey_final_starts_watched_temp
--ORDER BY visit_id, ep_order
;

-- Delete duplicates
DELETE FROM central_insights_sandbox.vbv_journey_final_starts_watched_temp2
WHERE added_page = 'added-page' AND content_id = duplicate_check;
SELECT * FROM central_insights_sandbox.vbv_journey_final_starts_watched_temp2 LIMIT 5;
-- Create a clean ep-order for ease of reading in the final table
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_final_starts_watched;
CREATE TABLE central_insights_sandbox.vbv_journey_final_starts_watched AS
SELECT destination,
       dt,
       unique_visitor_cookie_id,
       hashed_id,
       app_type,
       visit_id,
       row_number()
       over (PARTITION BY dt,unique_visitor_cookie_id,hashed_id,app_type, visit_id ORDER BY ep_order) AS episode_order,
       click_attribute,
       click_container,
       click_placement,
       click_user_experience,
       via_tleo,
       tleo_container,
       tleo_attribute,
       content_placement,
       content_id,
       playback_type,
       start_type,
       start_flag,
       watched_flag
FROM central_insights_sandbox.vbv_journey_final_starts_watched_temp2
;


------------------------------------------- Add in page and episode number ------------------------------------------------------------
-- For later analysis we need to know what episode number and what page number within a visit.
-- Join this with a table from the master script process

-- Get all the pages listed and given them a page count (i.e number page in visit) and a content count (the number of the content page within the visit)
DROP TABLE IF EXISTS central_insights_sandbox.vbv_content_id_table;
CREATE TABLE central_insights_sandbox.vbv_content_id_table AS
SELECT *, row_number() over (PARTITION BY destination, dt, hashed_id, visit_id ORDER BY page_count) as content_count
FROM (
         SELECT dt,
                destination,
                hashed_id,
                visit_id,
                page_type,
                content_id,
                row_number()
                over (PARTITION BY destination, dt, hashed_id, visit_id ORDER BY page_position::INT) as page_count
         FROM central_insights_sandbox.vbv_journey_page_type-- from the master table script
         ORDER BY visit_id, page_count)
WHERE page_type = 'episode_page'
   or page_type LIKE '%simulcast%';

-- Join the starts and watched table to the content table so that the page number within a visit can be included.
-- Use left join so that nothing is lost from the start table if it's not found in the other table or doesn't match because tracking is missing or there are data errors.
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_start_watch_complete_temp;
CREATE TABLE central_insights_sandbox.vbv_journey_start_watch_complete_temp AS
SELECT a.destination,
       a.dt,
--       a.unique_visitor_cookie_id,
       a.hashed_id,
       a.visit_id,
       a.app_type,
       a.episode_order AS episode_count, -- from stat/watched table
       b.page_count, -- from content_id table
       a.click_attribute,
       a.click_container,
       a.click_placement,
       a.click_user_experience,
       a.via_tleo,
       a.tleo_attribute,
       a.tleo_container,
       a.content_placement,
       a.content_id,
       a.playback_type,
       a.start_type,
       CAST (CASE
                     WHEN start_flag = 'iplxp-ep-started' THEN '1'
                     WHEN start_flag = 'no-start-flag' THEN '0'
                     ELSE '0' END  AS int) AS start_flag,
        CAST (CASE
                     WHEN watched_flag = 'iplxp-ep-watched' THEN '1'
                     WHEN watched_flag = 'no-watched-flag' THEN '0'
                      ELSE '0' END AS int) AS complete_flag
FROM central_insights_sandbox.vbv_journey_final_starts_watched a
         LEFT JOIN central_insights_sandbox.vbv_content_id_table b
                   ON a.dt = b.dt AND a.hashed_id = b.hashed_id AND a.visit_id = b.visit_id
                          AND a.episode_order = b.content_count
;



-- The master table only includes journeys up to 100 pages long. This table should also max out there
--SELECT count(*) FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp; --94,222,956 before delete, 94,049,062 after
DELETE FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp
WHERE episode_count > 100;



------------ 11. Tidy table ----------------
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set via_tleo = FALSE
WHERE via_tleo IS NULL;


-- Need to keep the TLEO information in one place. So any TLEO in click placement, move to the TLEO fields.
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set via_tleo = TRUE
WHERE click_placement = 'tleo_page';

UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set tleo_container = click_container
WHERE click_placement = 'tleo_page';

UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set tleo_attribute = click_attribute
WHERE click_placement = 'tleo_page';

UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set click_container = NULL
WHERE click_placement = 'tleo_page';

UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set click_attribute = NULL
WHERE click_placement = 'tleo_page';

UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
set click_placement = 'unknown'
WHERE click_placement = 'tleo_page';

--- Get container names in a better format
/*
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, '--', '-');
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, '...', '');
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, '\'', '');
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, '$-', '');
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, ' ', '-');
  */


-- Combine the two way of saying binge worthy and black british
UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, 'bingeworthy','binge-worthy');

UPDATE central_insights_sandbox.vbv_journey_start_watch_complete_temp
SET click_container = REPLACE(click_container, 'blackbritish','black-british');


 ---------------- 12. Add in user information  ----------------
 DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_start_watch_complete_temp_enriched;
 CREATE TABLE central_insights_sandbox.vbv_journey_start_watch_complete_temp_enriched AS
     SELECT a.*, b.age_range, b.frequency_band, b.frequency_group_aggregated
     FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp a
LEFT JOIN central_insights_sandbox.vbv_journey_test_users_uv b on a. dt = b.dt AND a.hashed_id =b.hashed_id and a.visit_id = b.visit_id;


/*
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_start_watch_complete;
CREATE TABLE central_insights_sandbox.vbv_journey_start_watch_complete AS
SELECT * FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp;
*/
INSERT INTO central_insights_sandbox.vbv_journey_start_watch_complete
SELECT * FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp_enriched;

GRANT ALL on central_insights_sandbox.vbv_journey_start_watch_complete TO GROUP dataforce_analysts;
GRANT SELECT ON central_insights_sandbox.vbv_journey_start_watch_complete TO GROUP central_insights_server;

------------------------------ END  ------------------------

GRANT SELECT ON central_insights_sandbox.vbv_journey_start_watch_complete TO GROUP dataforce_analysts;
GRANT SELECT ON central_insights_sandbox.vbv_journey_start_watch_complete TO GROUP central_insights_server;
GRANT SELECT ON central_insights_sandbox.vbv_journey_start_watch_complete TO christel_swift;

------------------------ Drop Tables ----------------------------
DROP TABLE if exists central_insights_sandbox.vbv_vmb_temp;
DROP TABLE if exists central_insights_sandbox.vbv_journey_test_users_uv;
DROP TABLE if exists central_insights_sandbox.vbv_temp_starts;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_content_clicks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_autoplay_clicks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_autoplay_web_complete;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_deeplinks_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_deeplinks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_view_all_clicks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_search_tleo_clicks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_content_clicks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_play_starts;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_and_starts;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_temp_starts;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_temp_clicks;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_valid_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_temp2;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_temp3;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_linked_starts_valid;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_starts_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_starts;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_play_watched;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_starts_and_watched;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_watched_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_watched_temp2;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_valid_watched;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_starts;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_watched;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched_dup_temp2;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_orphan_start_watched_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_final_starts_watched_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_first_episode_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_distinct_episodes;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_final_starts_watched_temp2;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_final_starts_watched;
DROP TABLE  IF EXISTS central_insights_sandbox.vbv_content_id_table;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_start_watch_complete_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_content_clicks_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_via_tleo_temp;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_clicks_via_tleo_valid;
DROP TABLE IF EXISTS central_insights_sandbox.vbv_journey_start_watch_complete_temp_enriched;

