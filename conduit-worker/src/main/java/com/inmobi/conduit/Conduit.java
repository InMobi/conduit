/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inmobi.conduit;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.inmobi.conduit.local.LocalStreamService;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.distcp.MergedStreamService;
import com.inmobi.conduit.distcp.MirrorStreamService;
import com.inmobi.conduit.purge.DataPurgerService;
import com.inmobi.conduit.utils.FileUtil;
import com.inmobi.conduit.utils.SecureLoginUtil;
import com.inmobi.conduit.zookeeper.CuratorLeaderManager;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

public class Conduit implements Service, ConduitConstants {
  private static Logger LOG = Logger.getLogger(Conduit.class);
  private ConduitConfig config;
  private String currentClusterName = null;
  private static int numStreamsLocalService = 5;
  private static volatile MessagePublisher publisher = null;
  private static int numStreamsMergeService = 5;
  private static int numStreamsMirrorService = 1;
  private static boolean isPurgerEnabled = true;
  private final Set<String> clustersToProcess;
  private final List<AbstractService> services = new ArrayList<AbstractService>();
  private volatile boolean stopRequested = false;
  private volatile boolean initFailed = false;
  private CuratorLeaderManager curatorLeaderManager = null;
  private volatile boolean conduitStarted = false;
  private static boolean isHCatEnabled = false;
  private static String hcatDBName = null;
  private static HiveConf hiveConf = null;

  public Conduit(ConduitConfig config, Set<String> clustersToProcess,
                 String currentCluster) {
    this(config, clustersToProcess);
    this.currentClusterName = currentCluster;
  }

  public Set<String> getClustersToProcess() {
    return clustersToProcess;
  }

  public Conduit(ConduitConfig config, Set<String> clustersToProcess) {
    this.config = config;
    this.clustersToProcess = clustersToProcess;
  }

  public ConduitConfig getConfig() {
    return config;
  }

  public static void setPublisher(MessagePublisher publisher) {
    Conduit.publisher = publisher;
  }

  public static MessagePublisher getPublisher() {
    return publisher;
  }

  public static String getHcatDBName() {
    return hcatDBName;
  }

  public static void setHcatDBName(String hcatDBName) {
    Conduit.hcatDBName = hcatDBName;
  }

  public static boolean isHCatEnabled() {
    return isHCatEnabled;
  }

  public static void setHCatEnabled(boolean enableHcat) {
    isHCatEnabled = enableHcat;
  }

  protected List<AbstractService> init() throws Exception {
    Cluster currentCluster = null;
    if (currentClusterName != null) {
      currentCluster = config.getClusters().get(currentClusterName);
    }

    // find the name of the jar containing UniformSizeInputFormat class.
    String inputFormatSrcJar = FileUtil.findContainingJar(
        com.inmobi.conduit.distcp.tools.mapred.UniformSizeInputFormat.class);
    LOG.debug("Jar containing UniformSizeInputFormat [" + inputFormatSrcJar + "]");

    // find the name of the jar containing AuditUtil class.
    String auditUtilSrcJar = FileUtil.findContainingJar(
        com.inmobi.messaging.util.AuditUtil.class);
    LOG.debug("Jar containing AuditUtil [" + auditUtilSrcJar + "]");
    for (Cluster cluster : config.getClusters().values()) {
      if (!clustersToProcess.contains(cluster.getName())) {
        continue;
      }
      //Start LocalStreamConsumerService for this cluster if it's the source of any stream
      if (cluster.getSourceStreams().size() > 0) {
        // copy input format jar from local to cluster FS
        copyInputFormatJarToClusterFS(cluster, inputFormatSrcJar);
        copyAuditUtilJarToClusterFs(cluster, auditUtilSrcJar);
        Iterator<String> iterator = cluster.getSourceStreams().iterator();
        Set<String> streamsToProcess = new HashSet<String>();
        while (iterator.hasNext()) {
          for (int i = 0; i < numStreamsLocalService && iterator.hasNext(); i++) {
            streamsToProcess.add(iterator.next());
          }
          if (streamsToProcess.size() > 0) {
            services.add(getLocalStreamService(config, cluster, currentCluster,
                streamsToProcess));
            streamsToProcess = new HashSet<String>();
          }
        }
      }

      Set<String> mergedStreamRemoteClusters = new HashSet<String>();
      Set<String> mirroredRemoteClusters = new HashSet<String>();
      Map<String, Set<String>> mergedSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      Map<String, Set<String>> mirrorSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
        //Start MergedStreamConsumerService instances for this cluster for each cluster
        //from where it has to fetch a partial stream and is hosting a primary stream
        //Start MirroredStreamConsumerService instances for this cluster for each cluster
        //from where it has to mirror mergedStreams

        if (cStream.isPrimary()) {
          // copy messaging-client-core jar from local to cluster FS
          copyAuditUtilJarToClusterFs(cluster, auditUtilSrcJar);
          for (String cName : config.getSourceStreams().get(cStream.getName())
              .getSourceClusters()) {
            mergedStreamRemoteClusters.add(cName);
            if (mergedSrcClusterToStreamsMap.get(cName) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mergedSrcClusterToStreamsMap.put(cName, tmp);
            } else {
              mergedSrcClusterToStreamsMap.get(cName).add(cStream.getName());
            }
          }
        }
        if (!cStream.isPrimary()) {
          // copy messaging-client-core jar from local to cluster FS
          copyAuditUtilJarToClusterFs(cluster, auditUtilSrcJar);
          Cluster primaryCluster = config.getPrimaryClusterForDestinationStream(cStream.getName());
          if (primaryCluster != null) {
            mirroredRemoteClusters.add(primaryCluster.getName());
            String clusterName = primaryCluster.getName();
            if (mirrorSrcClusterToStreamsMap.get(clusterName) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mirrorSrcClusterToStreamsMap.put(clusterName, tmp);
            } else {
              mirrorSrcClusterToStreamsMap.get(clusterName).add(
                  cStream.getName());
            }
          }
        }
      }


      for (String remote : mergedStreamRemoteClusters) {

        Iterator<String> iterator = mergedSrcClusterToStreamsMap.get(remote)
            .iterator();
        Set<String> streamsToProcess = new HashSet<String>();
        while (iterator.hasNext()) {
          for (int i = 0; i < numStreamsMergeService && iterator.hasNext(); i++) {
            streamsToProcess.add(iterator.next());
          }
          if (streamsToProcess.size() > 0) {
            services.add(getMergedStreamService(config, config.getClusters()
                .get(remote), cluster, currentCluster, streamsToProcess));
            streamsToProcess = new HashSet<String>();
          }
        }

      }
      for (String remote : mirroredRemoteClusters) {

        Iterator<String> iterator = mirrorSrcClusterToStreamsMap.get(remote)
            .iterator();
        Set<String> streamsToProcess = new HashSet<String>();
        while (iterator.hasNext()) {
          for (int i = 0; i < numStreamsMirrorService && iterator.hasNext(); i++) {
            streamsToProcess.add(iterator.next());
          }
          if (streamsToProcess.size() > 0) {
            services.add(getMirrorStreamService(config, config.getClusters()
                .get(remote), cluster, currentCluster, streamsToProcess));
            streamsToProcess = new HashSet<String>();
          }
        }

      }
    }

    //Start a DataPurgerService for this Cluster/Clusters to process
    Iterator<String> it = clustersToProcess.iterator();
    while (isPurgerEnabled && it.hasNext()) {
      String clusterName = it.next();
      Cluster cluster = config.getClusters().get(clusterName);
      LOG.info("Starting Purger for Cluster [" + clusterName + "]");
      //Start a purger per cluster
      services.add(new DataPurgerService(config, cluster));
    }
    if (isHCatEnabled) {
      prepareLastAddedPartitions();
    }
    return services;
  }

  public static HiveConf getHiveConf() {
    return hiveConf;
  }

  public static void setHiveConf(HiveConf hiveConf) {
    Conduit.hiveConf = hiveConf;
  }

  private void prepareLastAddedPartitions() {
    for (AbstractService service : services) {
      try {
        ((AbstractService) service).prepareLastAddedPartitionMap();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void copyInputFormatJarToClusterFS(Cluster cluster, 
      String inputFormatSrcJar) throws IOException {
    FileSystem clusterFS = FileSystem.get(cluster.getHadoopConf());
    // create jars path inside /conduit/system/tmp path
    Path jarsPath = new Path(cluster.getTmpPath(), "jars");
    if (!clusterFS.exists(jarsPath)) {
      clusterFS.mkdirs(jarsPath);
    }
    // copy inputFormat source jar into /conduit/system/tmp/jars path
    Path inputFormatJarDestPath = new Path(jarsPath, "conduit-distcp-current.jar");
    if (clusterFS.exists(inputFormatJarDestPath)) {
      clusterFS.delete(inputFormatJarDestPath, true);
    }
    clusterFS.copyFromLocalFile(new Path(inputFormatSrcJar), inputFormatJarDestPath);
  }

  private void copyAuditUtilJarToClusterFs(Cluster cluster,
      String auditUtilSrcJar) throws IOException {
    FileSystem clusterFS = FileSystem.get(cluster.getHadoopConf());
    // create jars path inside /conduit/system/tmp path
    Path jarsPath = new Path(cluster.getTmpPath(), "jars");
    if (!clusterFS.exists(jarsPath)) {
      clusterFS.mkdirs(jarsPath);
    }
    // copy AuditUtil source jar into /conduit/system/tmp/jars path
    Path AuditUtilJarDestPath = new Path(jarsPath, "messaging-client-core.jar");
    if (clusterFS.exists(AuditUtilJarDestPath)) {
      clusterFS.delete(AuditUtilJarDestPath, true);
    }
    clusterFS.copyFromLocalFile(new Path(auditUtilSrcJar), AuditUtilJarDestPath);
  }

  protected LocalStreamService getLocalStreamService(ConduitConfig config,
      Cluster cluster, Cluster currentCluster, Set<String> streamsToProcess)
          throws IOException {
    return new LocalStreamService(config, cluster, currentCluster,
        new FSCheckpointProvider(cluster.getCheckpointDir()), streamsToProcess);
  }

  protected MergedStreamService getMergedStreamService(ConduitConfig config,
      Cluster srcCluster, Cluster dstCluster, Cluster currentCluster,
      Set<String>  streamsToProcess)
          throws Exception {
    return new MergedStreamService(config, srcCluster, dstCluster,
        currentCluster,
        new FSCheckpointProvider(dstCluster.getCheckpointDir()),
        streamsToProcess);
  }

  protected MirrorStreamService getMirrorStreamService(ConduitConfig config,
      Cluster srcCluster, Cluster dstCluster, Cluster currentCluster,
      Set<String> streamsToProcess)
          throws Exception {
    return new MirrorStreamService(config, srcCluster, dstCluster,
        currentCluster,
        new FSCheckpointProvider(dstCluster.getCheckpointDir()),
        streamsToProcess);

  }

  @Override
  public void stop() throws Exception {
    stopRequested = true;
    if (conduitStarted) {
      synchronized (services) {
        for (AbstractService service : services) {
          LOG.info("Stopping [" + service.getName() + "]");
          service.stop();
        }
      }
    }
    if (curatorLeaderManager != null) {
      curatorLeaderManager.close();
    }
  }

  @Override
  public void join() throws Exception {
    for (AbstractService service : services) {
      LOG.info("Waiting for [" + service.getName() + "] to finish");
      service.join();
    }
    if (publisher != null) {
      publisher.close();
    }
    if (isHCatEnabled) {
      Hive.closeCurrent();
    }
    LOG.info("Conduit Shutdown complete..");
  }

  @Override
  public void start() throws Exception{
    startConduit();
    //If all threads are finished release leadership
    System.exit(0);
  }

  public void startConduit() throws Exception {
    try {
      synchronized (services) {
        if (stopRequested) {
          return;
        }
        init();
        for (AbstractService service : services) {
          service.start();
        }
      }
      conduitStarted = true;
    } catch (Throwable e) {
      initFailed = true;
      LOG.warn("Stopping conduit because of error in initializing conduit ", e);
    }

    // if there is any outstanding stop request meanwhile, handle it here
    if (stopRequested || initFailed) {
      stop();
    }
    // Block this method to avoid losing leadership of current work
    join();
  }

  private static String getProperty(Properties prop, String property) {
    String propvalue = prop.getProperty(property);
    if (new File(propvalue).exists()) {
      return propvalue;
    } else {
      String filePath = ClassLoader.getSystemResource(propvalue).getPath();
      if (new File(filePath).exists())
        return filePath;
    }
    return null;
  }

  private static MessagePublisher createMessagePublisher(Properties prop)
      throws IOException {
    String configFile = prop.getProperty(AUDIT_PUBLISHER_CONFIG_FILE);
    if (configFile != null) {
      try {
        ClientConfig config = ClientConfig.load(configFile);
        return MessagePublisherFactory.create(config);
      } catch (Exception e) {
        LOG.warn("Not able to create a publisher for a given configuration ", e);
      }
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    try {
      if (args.length != 1 ) {
        LOG.error("Usage: com.inmobi.conduit.Conduit <conduit.cfg>");
        throw new RuntimeException("Usage: com.inmobi.conduit.Conduit " +
            "<conduit.cfg>");
      }
      String cfgFile = args[0].trim();
      Properties prop = new Properties();
      prop.load(new FileReader(cfgFile));
      String purgerEnabled = prop.getProperty(PERGER_ENABLED);
      if (purgerEnabled != null)
        isPurgerEnabled = Boolean.parseBoolean(purgerEnabled);

      String streamperLocal = prop.getProperty(STREAMS_PER_LOCALSERVICE);
      if (streamperLocal != null) {
        numStreamsLocalService = Integer.parseInt(streamperLocal);
      }
      String streamperMerge = prop.getProperty(STREAMS_PER_MERGE);
      if (streamperMerge != null) {
        numStreamsMergeService = Integer.parseInt(streamperMerge);
      }
      String streamperMirror = prop.getProperty(STREAMS_PER_MIRROR);
      if (streamperMirror != null) {
        numStreamsMirrorService = Integer.parseInt(streamperMirror);
      }
      String numOfDirPerDistcpService = prop.getProperty(DIR_PER_DISTCP_PER_STREAM);
      if (numOfDirPerDistcpService != null) {
        System.setProperty(DIR_PER_DISTCP_PER_STREAM, numOfDirPerDistcpService);
      }

      String log4jFile = getProperty(prop, LOG4J_FILE);
      if (log4jFile == null) {
        LOG.error("log4j.properties incorrectly defined");
        throw new RuntimeException("Log4j.properties not defined");
      }
      PropertyConfigurator.configureAndWatch(log4jFile);
      LOG.info("Log4j Property File [" + log4jFile + "]");

      String clustersStr = prop.getProperty(CLUSTERS_TO_PROCESS);
      if (clustersStr == null || clustersStr.length() == 0) {
        LOG.error("Please provide " + CLUSTERS_TO_PROCESS + " in [" +
            cfgFile + "]");
        throw new RuntimeException("Insufficent information on cluster name");
      }
      String[] clusters = clustersStr.split(",");
      String conduitConfigFile = getProperty(prop, CONDUIT_XML);
      if (conduitConfigFile == null)  {
        LOG.error("Conduit Configuration file doesn't exist..can't proceed");
        throw new RuntimeException("Specified conduit config file doesn't " +
            "exist");
      }
      String zkConnectString = prop.getProperty(ZK_ADDR);
      if (zkConnectString == null || zkConnectString.length() == 0) {
        LOG.error("Zookeper connection string not specified");
        throw new RuntimeException("Zoookeeper connection string not " +
            "specified");
      }
      String enableZK = prop.getProperty(ENABLE_ZOOKEEPER);
      boolean enableZookeeper;
      if (enableZK != null && enableZK.length() != 0)
        enableZookeeper = Boolean.parseBoolean(enableZK);
      else
        enableZookeeper = true;
      String currentCluster = prop.getProperty(CLUSTER_NAME);

      String principal = prop.getProperty(KRB_PRINCIPAL);
      String keytab = getProperty(prop, KEY_TAB_FILE);

      String mbPerMapper = prop.getProperty(MB_PER_MAPPER);
      if (mbPerMapper != null) {
        System.setProperty(MB_PER_MAPPER, mbPerMapper);
      }
      String numRetries = prop.getProperty(NUM_RETRIES);
      if (numRetries != null) {
        System.setProperty(NUM_RETRIES, numRetries);
      }

      String numFilesPerLocalStream = prop.getProperty(
          FILES_PER_COLLECETOR_PER_LOCAL_STREAM);
      if (numFilesPerLocalStream != null) {
        System.setProperty(FILES_PER_COLLECETOR_PER_LOCAL_STREAM,
            numFilesPerLocalStream);
      }

      String timeoutToProcessLastCollectorFile = prop.getProperty(
          TIMEOUT_TO_PROCESS_LAST_COLLECTOR_FILE);
      if (timeoutToProcessLastCollectorFile != null) {
        System.setProperty(TIMEOUT_TO_PROCESS_LAST_COLLECTOR_FILE,
            timeoutToProcessLastCollectorFile);
      }

      //Init Conduit metrics
      try {
        ConduitMetrics.init(prop);
        ConduitMetrics.startAll();
      } catch (IOException e) {
        LOG.error("Exception during initialization of metrics" + e.getMessage());
      }

      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Security enabled, trying kerberoes login principal ["
            + principal + "] keytab [" + keytab + "]");
        //krb enabled
        if (principal != null && keytab != null) {
          SecureLoginUtil.login(KRB_PRINCIPAL, principal, KEY_TAB_FILE, keytab);
        }
        else  {
          LOG.error("Kerberoes principal/keytab not defined properly in " +
              "conduit.cfg");
          throw new RuntimeException("Kerberoes principal/keytab not defined " +
              "properly in conduit.cfg");
        }
      }

      // parse hcat properties
      parseHCatProperties(prop);

      ConduitConfigParser configParser =
          new ConduitConfigParser(conduitConfigFile);
      ConduitConfig config = configParser.getConfig();
      StringBuffer conduitClusterId = new StringBuffer();
      Set<String> clustersToProcess = new HashSet<String>();
      if (clusters.length == 1 && "ALL".equalsIgnoreCase(clusters[0])) {
        for (Cluster c : config.getClusters().values()) {
          clustersToProcess.add(c.getName());
        }
      } else {
        for (String c : clusters) {
          if (config.getClusters().get(c) == null) {
            LOG.warn("Cluster name is not found in the config - " + c);
            return;
          }
          clustersToProcess.add(c);
          conduitClusterId.append(c);
          conduitClusterId.append("_");
        }
      }
      final Conduit conduit = new Conduit(config, clustersToProcess,
          currentCluster);

      MessagePublisher msgPublisher = createMessagePublisher(prop);
      if (msgPublisher != null) {
        LOG.info("Audit feature is enabled for worker ");
        System.setProperty(AUDIT_ENABLED_KEY, "true");
      } else {
        /*
         * Disable the audit feature for worker in case if we are not able to create
         * a publisher from a given publisher configuration file
         */
        System.setProperty(AUDIT_ENABLED_KEY, "false");
      }
      conduit.setPublisher(msgPublisher);

      Signal.handle(new Signal("TERM"), new SignalHandler() {

        @Override
        public void handle(Signal signal) {
          try {
            LOG.info("Starting to stop conduit...");
            conduit.stop();
            ConduitMetrics.stopAll();
          }
          catch (Exception e) {
            LOG.warn("Error in shutting down conduit", e);
          }
        }
      });
      if (enableZookeeper) {
        LOG.info("Starting CuratorLeaderManager for leader election ");
        conduit.startCuratorLeaderManager(zkConnectString,
            conduitClusterId, conduit);
      } else {
        conduit.start();
      }
    }
    catch (Exception e) {
      LOG.warn("Error in starting Conduit daemon", e);
      throw new Exception(e);
    }
  }

  private static void parseHCatProperties(Properties prop) {
    String hcatEnabled = prop.getProperty(HCAT_ENABLED);
    if (hcatEnabled != null && Boolean.parseBoolean(hcatEnabled)) {
      LOG.info("HCAT is enabled for worker ");
      isHCatEnabled = true;
      String hcatDBName = prop.getProperty(HCAT_DATABASE_NAME);
      if (hcatDBName != null && !hcatDBName.isEmpty()) {
        Conduit.setHcatDBName(hcatDBName);
      } else {
        throw new RuntimeException("HCAT DataBase name is not specified"
            + " in the conduit config file");
      }
      constructHiveConf();
    } else {
      LOG.info("HCAT is not enabled for the worker ");
    }
    
  }

  private static void constructHiveConf() {
    HiveConf hConf = new HiveConf();
    String metastoreUrl = hConf.getVar(HiveConf.ConfVars.METASTOREURIS);
    if (metastoreUrl == null) {
      throw new RuntimeException("metastroe.uri property is not specified in hive-site.xml");
    }
    LOG.info("hive metastore uri is : " + metastoreUrl);
    hiveConf = hConf;
  }

  private void startCuratorLeaderManager(
      String zkConnectString, StringBuffer conduitClusterId,
      final Conduit conduit) throws Exception {
    curatorLeaderManager = new CuratorLeaderManager(
        conduit, conduitClusterId.toString(), zkConnectString);
    curatorLeaderManager.start();
  }

}
