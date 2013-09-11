CREATE OR REPLACE FUNCTION daily_insert_trigger_function()
RETURNS TRIGGER AS $$
DECLARE
 dt_var varchar;
 timeinterval bigint;
 dateresult date;
BEGIN
 timeinterval=NEW.timeinterval/1000;
 SELECT (TIMESTAMP WITH TIME ZONE 'epoch' + timeinterval * INTERVAL '1  second')::date into dateresult;
 select to_char(dateresult,'yyyymmdd') into dt_var ;
 execute'insert into audit_daily'||dt_var||  ' select  $1.*' using new;
 RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER daily_insert_trigger
  BEFORE INSERT ON daily_databus_summary
  FOR EACH ROW EXECUTE PROCEDURE daily_insert_trigger_function();