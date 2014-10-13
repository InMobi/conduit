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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.HCatClientUtil;

public class TestHCatUtil {

  private static final Log LOG = LogFactory.getLog(TestHCatUtil.class);

  static Thread hcatServer = null;
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

  public void stop() {
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
    return hcatConf;
  }

  public static HiveConf getHiveConf() {
    return Conduit.getHiveConf();
  }

  public Database createDatabase(String dbName) throws Exception {
      if(null == dbName) { return null; }
      Database db = new Database();
      db.setName(dbName);
      try {
        Hive.get().createDatabase(db);
      } catch(HiveException e) {
        //
      }
      return db;
  }
  public static HCatClientUtil getHCatUtil(HiveConf hiveConf) {
    String metaStoreUri = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
    HCatClientUtil hcatClientUtil = new HCatClientUtil(metaStoreUri);
    return hcatClientUtil;
  }

  public static void createHCatClients(HiveConf hcatConf, HCatClientUtil hcatClientUtil)
      throws InterruptedException {
    try {
      hcatClientUtil.createHCatClients(10, hcatConf);
    } catch (HCatException e) {
      LOG.warn("Exception occured while trying to create hcat cleints ", e);
    }
  }

  public static HCatClient getHCatClient(HCatClientUtil hcatClientUtil)
      throws InterruptedException {
    return hcatClientUtil.getHCatClient();
  }

  public static void createDataBase(String dbName, HCatClient hcatClient) {
    HCatCreateDBDesc dbDesc = null;
    try {
      dbDesc = HCatCreateDBDesc.create(dbName)
          .ifNotExists(true).build();
      hcatClient.createDatabase(dbDesc);
    } catch (HCatException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static List<HCatFieldSchema> getPartCols() throws HCatException {
    List<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
    ptnCols.add(new HCatFieldSchema("year", Type.STRING, "year column"));
    ptnCols.add(new HCatFieldSchema("month", Type.STRING, "month column"));
    ptnCols.add(new HCatFieldSchema("day", Type.STRING, "day column"));
    ptnCols.add(new HCatFieldSchema("hour", Type.STRING, "hour column"));
    ptnCols.add(new HCatFieldSchema("minute", Type.STRING, "minute column"));
    return ptnCols;
  }

  public void createTable(String dbName, String tableName) throws Exception {
    Hive hive = Hive.get();
    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    Map<String, String> params = new HashMap<String, String>();
    params.put("sd_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd = createStorageDescriptor(tableName, cols, params, serdParams);
    Table tbl = createTable(dbName, tableName, null, null,
        getPartKeys(), sd, 90);

  }

  private Table createTable(String dbName, String tblName, String owner,
      Map<String,String> tableParams, Map<String, String> partitionKeys,
      StorageDescriptor sd, int lastAccessTime) throws Exception {
    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
   
    if(owner != null) {
      tbl.setOwner(owner);
    }

    if(partitionKeys != null) {
      List<FieldSchema> partKeys = new ArrayList<FieldSchema>();
      Set<String> keySet = partitionKeys.keySet();
      Iterator<String> it = keySet.iterator();
      while (it.hasNext()) {
        partKeys.add(new FieldSchema(it.next(), "", ""));
      }
    }

    tbl.setLastAccessTime(lastAccessTime);

    Hive.get().createTable(tbl);
    return tbl;
  }

  public Map<String, String> getPartKeys() {
    Map<String, String> partitionKeys = new HashMap<String, String>();
    partitionKeys.put("year", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("month", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("day", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("hour", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("value", serdeConstants.STRING_TYPE_NAME);
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

  public static void createTable(HCatClient hCatClient, String dbName,
      String tableName, List<HCatFieldSchema> ptnCols) throws HCatException {

    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("stringcolumn", Type.STRING, "id columns"));

    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
        .create(dbName, tableName, cols).fileFormat("sequencefile")
        .partCols(ptnCols).build();
    hCatClient.createTable(tableDesc);
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

  public static void addPartition(HCatClient hCatClient, String dbName,
      String tableName, String location, Map<String, String> partSpec)
          throws HCatException {  
    HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
        tableName, location, partSpec).build();
    hCatClient.addPartition(addPtn);
  }

  public static void submitBack(HCatClientUtil hcatUtil, HCatClient hcatClient) {
    hcatUtil.addToPool(hcatClient);
  }
}