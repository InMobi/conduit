package com.inmobi.conduit;

public interface ConduitConfigParserTags {

  public static final String DEFAULTS = "defaults";
  public static final String ROOTDIR = "rootdir";
  public static final String RETENTION_IN_HOURS = "retentioninhours";
  public static final String TRASH_RETENTION_IN_HOURS = "trashretentioninhours";

  public static final String NAME = "name";
  public static final String STREAM = "stream";
  public static final String SOURCE = "source";
  public static final String DESTINATION = "destination";
  public static final String PRIMARY = "primary";

  public static final String CLUSTER = "cluster";
  public static final String JOB_QUEUE_NAME = "jobqueuename";
  public static final String HDFS_URL = "hdfsurl";
  public static final String JT_URL = "jturl";
  public static final String COPYMAPPER_IMPL="copyMapperClass";
  String CLUSTER_READ_URL="readUrl";
  public static final String HCAT_ENABLED_PER_STREAM = "hcatenabled";
  public static final String IS_ENABLED_STREAM = "isenabled";
}
