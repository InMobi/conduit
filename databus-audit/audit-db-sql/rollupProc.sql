CREATE OR REPLACE FUNCTION rollup(srcTable text, destTable text, masterTable text, startTime bigint, endTime bigint, intervalLength bigint)
RETURNS void AS
$BODY$
DECLARE
 tmpstart bigint := startTime;
 tmpend bigint := startTime + intervalLength;
 query text;
 createTable text;
 rec record;
 selectquery text;
 valueString text;
 disinherit text;
 inherit text;
BEGIN
createTable = 'CREATE TABLE ' || destTable || '( LIKE ' || srcTable || ' INCLUDING ALL)';
EXECUTE createTable;
while (tmpend < endTime) LOOP
selectquery = 'select tier,topic,hostname,cluster,sum(sent) as sent, sum(c0) as c0, sum(c1) as c1, sum(c2) as c2, sum(c3) as c3, sum(c4) as c4, sum(c5) as c5, sum(c6) as c6, sum(c7) as c7, sum(c8) as c8, sum(c9) as c9, sum(c10) as c10, sum(c15) as c15, sum(c30) as c30, sum(c60) as c60, sum(c120) as c120, sum(c240) as c240, sum(c600) as c600 from ' || srcTable || ' where timeinterval >= ' || tmpstart || ' and timeinterval < ' || tmpend || ' group by tier,topic,hostname,cluster';
FOR rec IN EXECUTE selectquery LOOP
valueString = tmpstart||','||quote_literal(rec.tier)||','||quote_literal(rec.topic)||','||quote_literal(rec.hostname)||','||quote_literal(rec.cluster)||','||rec.sent||','||rec.c0||','||rec.c1||','||rec.c2||','||rec.c3||','||rec.c4||','||rec.c5||','||rec.c6||','||rec.c7||','||rec.c8||','||rec.c9||','||rec.c10||','||rec.c15||','||rec.c30||','||rec.c60||','||rec.c120||','||rec.c240||','||rec.c600;
query = 'insert into '|| destTable || '(timeinterval,tier,topic,hostname,cluster,sent,c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c15,c30,c60,c120,c240,c600) values('||valueString||')';
EXECUTE query;
END LOOP;
tmpstart=tmpend;
tmpend=tmpend+intervalLength;
END LOOP;
disinherit = 'ALTER TABLE ' || srcTable || ' NO INHERIT ' || masterTable ;
EXECUTE disinherit;
inherit = 'ALTER TABLE ' || destTable || ' INHERIT ' || masterTable ;
EXECUTE inherit;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION createDailyTable(masterTable text, dayTable text, currentTimeStamp text)
RETURNS void AS
$BODY$
DECLARE
 createTable text;
 checkconstraint text;
 index text;
BEGIN
checkconstraint = 'CHECK(timeinterval >= extract (epoch from timestamp ' || quote_literal(currentTimeStamp) || ')*1000::bigint AND timeinterval < extract(epoch from timestamp ' || quote_literal(currentTimeStamp) || ')*1000::bigint )';
createTable = 'CREATE TABLE IF NOT EXISTS ' || dayTable || '(' || checkconstraint || ') INHERITS (' || masterTable || ')';
EXECUTE createTable;
index = 'CREATE INDEX ' || dayTable || '_idx ON ' || dayTable || ' USING btree(timeinterval)';
EXECUTE index;
index = 'CREATE INDEX ' || dayTable || '_clusteridx ON ' || dayTable || ' USING btree(cluster)';
EXECUTE index;
index = 'CREATE INDEX ' || dayTable || '_topicidx ON ' || dayTable || ' USING  btree(topic)';
EXECUTE index;
index = 'CREATE INDEX ' || dayTable || '_hostnameidx ON ' || dayTable || ' USING btree(hostname)';
EXECUTE index;
index = 'CREATE INDEX ' || dayTable || '_tieridx ON ' || dayTable || ' USING btree(tier)';
EXECUTE index;
END
$BODY$
LANGUAGE plpgsql;