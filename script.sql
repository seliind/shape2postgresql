CREATE TABLE IF NOT EXISTS public.osm_log
(
    gid serial primary key,
    log_date timestamp without time zone,
    status character varying(50) ,
    message text ,
    currentdate timestamp without time zone DEFAULT CURRENT_TIMESTAMP
)
