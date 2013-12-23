-- Table: daily_conduit_summary

-- DROP TABLE daily_conduit_summary;

CREATE TABLE daily_conduit_summary
(
  timeinterval bigint NOT NULL,
  hostname character varying(50) NOT NULL,
  tier character varying(30) NOT NULL,
  topic character varying(25) NOT NULL,
  cluster character varying(50) NOT NULL,
  sent bigint,
  c0 bigint,
  c1 bigint,
  c2 bigint,
  c3 bigint,
  c4 bigint,
  c5 bigint,
  c6 bigint,
  c7 bigint,
  c8 bigint,
  c9 bigint,
  c10 bigint,
  c15 bigint,
  c30 bigint,
  c60 bigint,
  c120 bigint,
  c240 bigint,
  c600 bigint,
  CONSTRAINT daily_conduit_summary_pkey PRIMARY KEY (timeinterval, hostname, tier, topic, cluster)
)
WITH (
  OIDS=FALSE
);

ALTER TABLE daily_conduit_summary
  OWNER TO conduit_user;

GRANT ALL ON TABLE daily_conduit_summary TO conduit_user;
GRANT SELECT ON TABLE daily_conduit_summary TO dbreadonly;

-- Trigger: daily_conduit_insert_trigger on daily_conduit_summary

-- DROP TRIGGER daily_conduit_insert_trigger ON daily_conduit_summary;

CREATE TRIGGER daily_conduit_insert_trigger
  BEFORE INSERT
  ON daily_conduit_summary
  FOR EACH ROW
  EXECUTE PROCEDURE daily_conduit_insert_trigger_function();


