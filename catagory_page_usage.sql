-- In journey work we should now see category pages being tracked properly
SELECT distinct app_type, click_placement, click_container, count(*) as clicks
FROM central_insights_sandbox.vb_journey_start_watch_complete
WHERE dt between 20210201 and 20210221
AND click_placement = 'categories_page'
GROUP BY 1,2,3
ORDER BY 4 desc;

DROP TABLE vb_cats_conts;
CREATE TEMP TABLE vb_cats_conts AS
SELECT DISTINCT CASE
                    WHEN metadata ILIKE '%bigscreen%' THEN 'tv'
                    WHEN metadata ILIKE '%responsive%' THEN 'web'
                    WHEN metadata ILIKE '%mobile%' THEN 'mobile'
                    ELSE 'unknown' END        as platform,
                split_part(placement, '.', 4) as category,
                container
FROM s3_audience.publisher
WHERE dt between 20210121 AND 20210221
  AND destination = 'PS_IPLAYER'
  AND placement ILIKE '%categories%'
  AND container ILIKE '%module%';

SELECT * FROM vb_cats_conts;

-- re-run journey script but without similfying the category name to give this table
SELECT destination,
       dt,
       hashed_id,
       visit_id,
       app_type,
       episode_count,
       page_count,
       click_attribute,
       click_container,
       click_placement,
       split_part(click_placement, '.', 4) as category,
       content_id,
       playback_type,
       start_type,
       start_flag,
       complete_flag,
       age_range,
       frequency_band,
       frequency_group_aggregated
FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp_enriched
WHERE  click_placement ILIKE '%categories%'
  AND click_container ILIKE '%module%'
LIMIT 10;

CREATE TABLE dataforce_sandbox.vb_cats AS
SELECT destination,
       dt,
       hashed_id,
       visit_id,
       app_type,
       episode_count,
       page_count,
       click_attribute,
       click_container,
       click_placement,
       split_part(click_placement, '.', 4) as category,
       content_id,
       playback_type,
       start_type,
       start_flag,
       complete_flag,
       age_range,
       frequency_band,
       frequency_group_aggregated
FROM central_insights_sandbox.vbv_journey_start_watch_complete_temp_enriched
WHERE  click_placement ILIKE '%categories%'
  AND click_container ILIKE '%module%'
;

SELECT app_type, category, count(*) as clicks, sum(start_flag) as starts, sum(complete_flag) as completes
FROM dataforce_sandbox.vb_cats
WHERE app_type IS NOT NULL
GROUP BY 1,2;

SELECT click_container, count(*) as clicks, sum(start_flag) as starts, sum(complete_flag) as completes
FROM dataforce_sandbox.vb_cats
--WHERE category = 'drama_and_soaps'
GROUP BY 1
ORDER BY 2 DESC;