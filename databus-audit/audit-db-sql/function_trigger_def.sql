--Below is the function and trigger definitions for inserting data into right partitons for daily_databus_summary table

CREATE OR REPLACE FUNCTION daily_databus_insert_trigger_function()
RETURNS TRIGGER AS $$
DECLARE                                                                                                 
  dt_var varchar;                                                                                        
  timeinterval bigint;                                                                                   
  dateresult date;                                                                                       
  BEGIN                                                                                                  
  timeinterval=NEW.timeinterval/1000;                                                                    
  SELECT (TIMESTAMP WITH TIME ZONE 'epoch' + timeinterval * INTERVAL '1  second')::date into dateresult; 
  select to_char(dateresult,'yyyymmdd') into dt_var ;                                                    
  execute'insert into daily_databus_summary'||dt_var||  ' select  $1.*' using new;                       
  RETURN NULL;                                                                                           
  END;      
$$
LANGUAGE plpgsql;

-- Trigger: daily_databus_insert_trigger on daily_databus_summary

-- DROP TRIGGER daily_databus_insert_trigger ON daily_databus_summary;

CREATE TRIGGER daily_databus_insert_trigger BEFORE INSERT ON daily_databus_summary FOR EACH ROW EXECUTE PROCEDURE daily_databus_insert_trigger_function();
