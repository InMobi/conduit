package com.inmobi.conduit.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde.serdeConstants;

import com.inmobi.conduit.Cluster;

public class TestHCatUtil {

  private static final Log LOG = LogFactory.getLog(TestHCatUtil.class);

  static Thread hcatServer = null;
  static HiveConf hiveConf = null;
  public static void startMetaStoreServer(final HiveConf hiveConf,
      final int msPort) {
    hcatServer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(msPort, null, hiveConf);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    });
    hcatServer.start();
  }

  public static void stop() {
    hcatServer.stop();
  }

  public static HiveConf getHcatConf(int msPort, String metaStoreWarehouseDir, String metaDb) {
    HiveConf hcatConf = new HiveConf();
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.set("hive.metastore.warehouse.dir", new File(metaStoreWarehouseDir).getAbsolutePath());
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hcatConf.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=target/test/" + metaDb +";create=true");
    hcatConf.set("javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver");
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars. METASTORETHRIFTFAILURERETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60);
    setHiveConf(hcatConf);
    return hcatConf;
  }

  public static HiveConf getHiveConf() {
    return hiveConf;
  }

  public static void setHiveConf(HiveConf hcatConf) {
    hiveConf = hcatConf;
  }
  public static Database createDatabase(String dbName) throws Exception {
    if(null == dbName) { return null; }
    Database db = new Database();
    db.setName(dbName);
    try {
      Hive.get(getHiveConf()).createDatabase(db);
    } catch(HiveException e) {
      LOG.warn("AAAAAAAAAAAAAAAAAAAAAAAAAA hive instance is in catch ", e);  
    }
    return db;
  }

  public org.apache.hadoop.hive.ql.metadata.Table createTable(String dbName,
      String tableName) throws Exception {
    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    Map<String, String> params = new HashMap<String, String>();
    params.put("sd_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd = createStorageDescriptor(tableName, cols, params, serdParams);
    Table tbl = createTable(dbName, tableName, "owner", null,
        getPartKeys(), sd, 90);
    org.apache.hadoop.hive.ql.metadata.Table table = new org.apache.hadoop.hive.ql.metadata.Table();
    table.setTTable(tbl);
    return table;
  }

  private Table createTable(String dbName, String tblName, String owner,
      Map<String,String> tableParams, Map<String, String> partitionKeys,
      StorageDescriptor sd, int lastAccessTime) throws Exception {
    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    if(tableParams != null) {
      tbl.setParameters(tableParams);
    }

    if(owner != null) {
      tbl.setOwner(owner);
    }

    if(partitionKeys != null) {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>(partitionKeys.size()));
      for(String key : partitionKeys.keySet()) {
        tbl.getPartitionKeys().add(
            new FieldSchema(key, partitionKeys.get(key), ""));
      }
    }
    tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
    try {
      LOG.info("Creating instance of storage handler to get input/output, serder info.");
      HiveStorageHandler sh = HiveUtils.getStorageHandler(getHiveConf(),
          DefaultStorageHandler.class.getName());
      sd.setInputFormat(sh.getInputFormatClass().getName());
      sd.setOutputFormat(sh.getOutputFormatClass().getName());
      sd.getSerdeInfo().setSerializationLib(
          sh.getSerDeClass().getName());
      tbl.putToParameters(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
          DefaultStorageHandler.class.getName());
    } catch (HiveException e) {
      throw new HiveException(
          "Exception while creating instance of storage handler",
          e);
    }
    tbl.setSd(sd);
    tbl.setLastAccessTime(lastAccessTime);
    org.apache.hadoop.hive.ql.metadata.Table table = new org.apache.hadoop.hive.ql.metadata.Table();
    table.setTTable(tbl);
    Hive hive = Hive.get(getHiveConf());
    LOG.warn("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA hive instance is  " + hive);

    hive.createTable(table);
    return tbl;
  } 

  public Map<String, String> getPartKeys() {
    Map<String, String> partitionKeys = new HashMap<String, String>();
    partitionKeys.put("year", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("month", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("day", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("hour", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("minute", serdeConstants.STRING_TYPE_NAME);
    return partitionKeys;
  }

  private StorageDescriptor createStorageDescriptor(String tableName,
      List<FieldSchema> cols, Map<String, String> params, Map<String, String> serdParams)  {
    StorageDescriptor sd = new StorageDescriptor();

    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(params);
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tableName);
    sd.getSerdeInfo().setParameters(serdParams);
    sd.getSerdeInfo().getParameters()
    .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());

    return sd;
  }

  public static Map<String, String> getPartitionMap(Calendar cal) {
    String dateStr = Cluster.getDateAsYYYYMMDDHHMNPath(cal.getTime().getTime());
    String [] dateSplits = dateStr.split(File.separator);
    Map<String, String> partSpec = new HashMap<String, String>();
    if (dateSplits.length == 5) {
      partSpec.put("year", dateSplits[0]);
      partSpec.put("month", dateSplits[1]);
      partSpec.put("day", dateSplits[2]);
      partSpec.put("hour", dateSplits[3]);
      partSpec.put("minute", dateSplits[4]);
    }
    return partSpec;
  }

  public static void addPartition(org.apache.hadoop.hive.ql.metadata.Table table,
      Map<String, String> partSpec) throws HiveException {
    Hive.get().createPartition(table, partSpec);
  }
}