package com.inmobi.databus.visualization.server;

import java.io.File;
import java.io.FileFilter;
import java.util.*;

import com.inmobi.messaging.ClientConfig;
import org.apache.log4j.Logger;

import com.inmobi.databus.DatabusConfig;
import com.inmobi.databus.DatabusConfigParser;
import com.inmobi.databus.audit.Tier;
import com.inmobi.databus.audit.Tuple;
import com.inmobi.databus.audit.query.AuditDbQuery;
import com.inmobi.databus.visualization.server.util.ServerDataHelper;

public class DataServiceManager {

  private static Logger LOG = Logger.getLogger(DataServiceManager.class);
  private static DataServiceManager instance = null;
  private List<DatabusConfig> dataBusConfig;
  private VisualizationProperties properties;
  private String feederPropertiesPath;

  protected DataServiceManager(boolean init) {
    this(init, null, null);
  }

  protected DataServiceManager(boolean init,
                               String visualizationPropertiesPath,
                               String feederPropertiesPath) {
    if (feederPropertiesPath == null || feederPropertiesPath.length() == 0) {
      feederPropertiesPath = ServerConstants.FEEDER_PROPERTIES_DEFAULT_PATH;
    }
    this.feederPropertiesPath = feederPropertiesPath;
    properties = new VisualizationProperties(visualizationPropertiesPath);
    if (init) {
      String folderPath = properties.get(ServerConstants.DATABUS_XML_PATH);
      initConfig(folderPath);
    }
  }

  protected void initConfig(String folderPath) {
    File folder = new File(folderPath);
    File[] xmlFiles = folder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        if (file.getName().toLowerCase().endsWith(".xml")) {
          return true;
        }
        return false;
      }
    });
    LOG.info("Databus xmls included in the conf folder:");
    dataBusConfig = new ArrayList<DatabusConfig>();
    for (File file : xmlFiles) {
      String fullPath = file.getAbsolutePath();
      LOG.info("File:" + fullPath);
      try {
        DatabusConfigParser parser = new DatabusConfigParser(fullPath);
        dataBusConfig.add(parser.getConfig());
      } catch (Exception e) {
        LOG.error("Exception while intializing DatabusConfigParser: ", e);
      }
    }
  }

  public static DataServiceManager get(boolean init) {
    if (instance == null) {
      instance = new DataServiceManager(init);
    }
    return instance;
  }

  public String getStreamAndClusterList() {
    Set<String> streamSet = new TreeSet<String>();
    Set<String> clusterSet = new TreeSet<String>();
    for (DatabusConfig config : dataBusConfig) {
      streamSet.addAll(config.getSourceStreams().keySet());
      clusterSet.addAll(config.getClusters().keySet());
    }
    streamSet.remove(ServerConstants.AUDIT_STREAM);
    List<String> streamList = new ArrayList<String>(streamSet);
    List<String> clusterList = new ArrayList<String>(clusterSet);
    streamList.add(0, "All");
    clusterList.add(0, "All");
    LOG.info("Returning stream list:" + streamList + " and cluster list:" +
        clusterList);
    String serverJson =
        ServerDataHelper.getInstance().setLoadMainPanelResponse(streamList,
            clusterList, properties);
    return serverJson;
  }

  public String getData(String filterValues) {
    Map<NodeKey, Node> nodeMap = new HashMap<NodeKey, Node>();
    Map<String, String> filterMap = getFilterMap(filterValues);
    String filterString = setFilterString(filterMap);
    ClientConfig config = ClientConfig.load(feederPropertiesPath);
    AuditDbQuery dbQuery =
        new AuditDbQuery(filterMap.get(ServerConstants.END_TIME_FILTER),
            filterMap.get(ServerConstants.START_TIME_FILTER), filterString,
            ServerConstants.GROUPBY_STRING, ServerConstants.TIMEZONE,
            properties.get(ServerConstants.PERCENTILE_STRING), config);
    try {
      dbQuery.execute();
    } catch (Exception e) {
      LOG.error("Exception while executing query: ", e);
    }
    LOG.info("Audit query: " + dbQuery.toString());
    try {
      dbQuery.displayResults();
    } catch (Exception e) {
      LOG.error("Exception while displaying results: ", e);
    }
    Set<Tuple> tupleSet = dbQuery.getTupleSet();
    Set<Float> percentileSet = dbQuery.getPercentileSet();
    Map<Tuple, Map<Float, Integer>> tuplesPercentileMap =
        dbQuery.getPercentile();
    LOG.debug("Percentile Set:" + percentileSet);
    LOG.debug("Tuples Percentile Map:" + tuplesPercentileMap);
    for (Tuple tuple : tupleSet) {
      createNode(tuple, nodeMap, percentileSet, tuplesPercentileMap.get(tuple));
    }
    buildPercentileMapOfAllNodes(nodeMap);
    addVIPNodesToNodesList(nodeMap, percentileSet);
    checkAndSetSourceListForMergeMirror(nodeMap);
    LOG.debug("Printing node list");
    for (Node node : nodeMap.values()) {
      LOG.debug("Final node :" + node);
    }
    Map<Tuple, Map<Float, Integer>> tierLatencyMap = getTierLatencyMap
        (feederPropertiesPath, filterMap.get(ServerConstants.END_TIME_FILTER),
            filterMap.get(ServerConstants.START_TIME_FILTER), filterString);/*
    return ServerDataHelper.getInstance().setGraphDataResponse(nodeMap,
        tierLatencyMap, properties);*/
    return "{\n" +
        "  \"nodes\": [{\n" +
        "    \"aggregatereceived\": \"81159\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 81159\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"81159\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 81159\n" +
        "    }],\n" +
        "    \"name\": \"tzgs4103.grid.uj1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"61548\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 61548\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"61548\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 61548\n" +
        "    }],\n" +
        "    \"name\": \"ergs4101.grid.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"63\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 63\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"63\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 63\n" +
        "    }],\n" +
        "    \"name\": \"opgs4101.grid.hkg1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"107232\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 107232\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"107232\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 107232\n" +
        "    }],\n" +
        "    \"name\": \"ergs4109.grid.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"62007\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 62007\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"62007\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 62007\n" +
        "    }],\n" +
        "    \"name\": \"erdc4001.grid.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"62007\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 62007\n" +
        "    }],\n" +
        "    \"name\": \"web2021.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"55980\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 55980\n" +
        "    }],\n" +
        "    \"name\": \"web2021.ads.ua2.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"63\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 63\n" +
        "    }],\n" +
        "    \"name\": \"web2014.ads.hkg1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"46258\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 46258\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"46258\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 46258\n" +
        "    }],\n" +
        "    \"name\": \"web2013.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"60974\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 60974\n" +
        "    }],\n" +
        "    \"name\": \"web2020.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"73\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 73\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"73\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 73\n" +
        "    }],\n" +
        "    \"name\": \"opgs4103.grid.hkg1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"61548\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 61548\n" +
        "    }],\n" +
        "    \"name\": \"web2008.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"81159\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 81159\n" +
        "    }],\n" +
        "    \"name\": \"web2020.ads.uj1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"86344\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 86344\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"86344\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 86344\n" +
        "    }],\n" +
        "    \"name\": \"web2013.ads.ua2.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"73\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 73\n" +
        "    }],\n" +
        "    \"name\": \"web2015.ads.hkg1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"86344\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 86344\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"86344\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 86344\n" +
        "    }],\n" +
        "    \"name\": \"gsdc3003.red.ua2.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"46258\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 46258\n" +
        "    }],\n" +
        "    \"name\": \"web2013.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"85351\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 85351\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"85351\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 85351\n" +
        "    }],\n" +
        "    \"name\": \"web2013.ads.uj1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"85351\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 85351\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"85351\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 85351\n" +
        "    }],\n" +
        "    \"name\": \"tzgs4105.grid.uj1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"230787\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"erdc4001.grid.lhr1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"ergs4101.grid.lhr1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"ergs4109.grid.lhr1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"erdc4001.grid.lhr1.inmobi.com\",\n" +
        "      \"messages\": 62007\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"ergs4101.grid.lhr1.inmobi.com\",\n" +
        "      \"messages\": 61548\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"ergs4109.grid.lhr1.inmobi.com\",\n" +
        "      \"messages\": 107232\n" +
        "    }],\n" +
        "    \"name\": \"lhr1\",\n" +
        "    \"tier\": \"hdfs\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"230787\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 230787\n" +
        "    }],\n" +
        "    \"name\": \"lhr1\",\n" +
        "    \"tier\": \"local\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"81159\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 81159\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"81159\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 81159\n" +
        "    }],\n" +
        "    \"name\": \"web2020.ads.uj1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"63\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 0\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 63\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"63\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 63\n" +
        "    }],\n" +
        "    \"name\": \"web2014.ads.hkg1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 0\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"55980\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 55980\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"55980\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 55980\n" +
        "    }],\n" +
        "    \"name\": \"gsdc3001.red.ua2.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"79536\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 79536\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"79536\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 79536\n" +
        "    }],\n" +
        "    \"name\": \"web2010.ads.uj1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"60974\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 60974\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"60974\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 60974\n" +
        "    }],\n" +
        "    \"name\": \"web2020.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"86344\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 86344\n" +
        "    }],\n" +
        "    \"name\": \"web2013.ads.ua2.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"246046\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 246046\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"246046\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 246046\n" +
        "    }],\n" +
        "    \"name\": \"uj1\",\n" +
        "    \"tier\": \"VIP\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"79536\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 79536\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"79536\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 79536\n" +
        "    }],\n" +
        "    \"name\": \"tzgs4101.grid.uj1.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"136\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 136\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"136\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 136\n" +
        "    }],\n" +
        "    \"name\": \"hkg1\",\n" +
        "    \"tier\": \"VIP\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"136\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"opgs4101.grid.hkg1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"opgs4103.grid.hkg1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"opgs4101.grid.hkg1.inmobi.com\",\n" +
        "      \"messages\": 63\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"opgs4103.grid.hkg1.inmobi.com\",\n" +
        "      \"messages\": 73\n" +
        "    }],\n" +
        "    \"name\": \"hkg1\",\n" +
        "    \"tier\": \"hdfs\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"136\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 136\n" +
        "    }],\n" +
        "    \"name\": \"hkg1\",\n" +
        "    \"tier\": \"local\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"55980\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 55980\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"55980\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 55980\n" +
        "    }],\n" +
        "    \"name\": \"web2021.ads.ua2.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"193006\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 193006\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"193006\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 193006\n" +
        "    }],\n" +
        "    \"name\": \"ua2\",\n" +
        "    \"tier\": \"VIP\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"230787\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 230787\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"230787\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 230787\n" +
        "    }],\n" +
        "    \"name\": \"lhr1\",\n" +
        "    \"tier\": \"VIP\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"73\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 73\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"73\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 73\n" +
        "    }],\n" +
        "    \"name\": \"web2015.ads.hkg1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"hkg1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"85351\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 85351\n" +
        "    }],\n" +
        "    \"name\": \"web2013.ads.uj1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"50682\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 50682\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"50682\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 50682\n" +
        "    }],\n" +
        "    \"name\": \"web2020.ads.ua2.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"62007\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 62007\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"62007\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 62007\n" +
        "    }],\n" +
        "    \"name\": \"web2021.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"61548\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 61548\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"61548\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 61548\n" +
        "    }],\n" +
        "    \"name\": \"web2008.ads.lhr1.inmobi.com\",\n" +
        "    \"tier\": \"agent\",\n" +
        "    \"cluster\": \"lhr1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"79536\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 79536\n" +
        "    }],\n" +
        "    \"name\": \"web2010.ads.uj1.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"50682\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 50682\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"50682\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 50682\n" +
        "    }],\n" +
        "    \"name\": \"rbgs4102.grid.ua2.inmobi.com\",\n" +
        "    \"tier\": \"collector\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"193006\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"rbgs4102.grid.ua2.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"gsdc3003.red.ua2.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"gsdc3001.red.ua2.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"rbgs4102.grid.ua2.inmobi.com\",\n" +
        "      \"messages\": 50682\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"gsdc3003.red.ua2.inmobi.com\",\n" +
        "      \"messages\": 86344\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"gsdc3001.red.ua2.inmobi.com\",\n" +
        "      \"messages\": 55980\n" +
        "    }],\n" +
        "    \"name\": \"ua2\",\n" +
        "    \"tier\": \"hdfs\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"669975\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"uj1\",\n" +
        "      \"messages\": 246046\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"ua2\",\n" +
        "      \"messages\": 193006\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"lhr1\",\n" +
        "      \"messages\": 230787\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"hkg1\",\n" +
        "      \"messages\": 136\n" +
        "    }],\n" +
        "    \"name\": \"ua2\",\n" +
        "    \"tier\": \"merge\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"source\": [\"ua2\", \"lhr1\", \"uj1\"],\n" +
        "    \"topicSource\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"sourceList\": [\"hkg1\", \"ua2\", \"lhr1\", \"uj1\"]\n" +
        "    }],\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"669975\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"uj1\",\n" +
        "      \"messages\": 669975\n" +
        "    }],\n" +
        "    \"name\": \"ua2_main\",\n" +
        "    \"tier\": \"mirror\",\n" +
        "    \"cluster\": \"ua2_main\",\n" +
        "    \"source\": [\"ua2\"],\n" +
        "    \"topicSource\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"sourceList\": [\"ua2\"]\n" +
        "    }],\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"193006\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 193006\n" +
        "    }],\n" +
        "    \"name\": \"ua2\",\n" +
        "    \"tier\": \"local\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"246046\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"tzgs4103.grid.uj1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"tzgs4105.grid.uj1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"tzgs4101.grid.uj1.inmobi.com\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"tzgs4103.grid.uj1.inmobi.com\",\n" +
        "      \"messages\": 81159\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"tzgs4105.grid.uj1.inmobi.com\",\n" +
        "      \"messages\": 85351\n" +
        "    }, {\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"hostname\": \"tzgs4101.grid.uj1.inmobi.com\",\n" +
        "      \"messages\": 79536\n" +
        "    }],\n" +
        "    \"name\": \"uj1\",\n" +
        "    \"tier\": \"hdfs\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"246046\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 2\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 2\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 246046\n" +
        "    }],\n" +
        "    \"name\": \"uj1\",\n" +
        "    \"tier\": \"local\",\n" +
        "    \"cluster\": \"uj1\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 2\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 2\n" +
        "      }]\n" +
        "    }]\n" +
        "  }, {\n" +
        "    \"aggregatereceived\": \"50682\",\n" +
        "    \"overallLatency\": [{\n" +
        "      \"percentile\": 90,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 80,\n" +
        "      \"latency\": 0\n" +
        "    }, {\n" +
        "      \"percentile\": 99,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 95,\n" +
        "      \"latency\": 1\n" +
        "    }, {\n" +
        "      \"percentile\": 99.9,\n" +
        "      \"latency\": 1\n" +
        "    }],\n" +
        "    \"senttopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 0\n" +
        "    }],\n" +
        "    \"aggregatesent\": \"0\",\n" +
        "    \"receivedtopicStatsList\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"messages\": 50682\n" +
        "    }],\n" +
        "    \"name\": \"web2020.ads.ua2.inmobi.com\",\n" +
        "    \"tier\": \"publisher\",\n" +
        "    \"cluster\": \"ua2\",\n" +
        "    \"topicLatency\": [{\n" +
        "      \"topic\": \"ifc_ir\",\n" +
        "      \"percentileLatencyList\": [{\n" +
        "        \"percentile\": 90,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 80,\n" +
        "        \"latency\": 0\n" +
        "      }, {\n" +
        "        \"percentile\": 99,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 95,\n" +
        "        \"latency\": 1\n" +
        "      }, {\n" +
        "        \"percentile\": 99.9,\n" +
        "        \"latency\": 1\n" +
        "      }]\n" +
        "    }]\n" +
        "  }]\n" +
        "}";
  }

  protected Map<String, String> getFilterMap(String filterValues) {
    Map<String, String> filterMap = new HashMap<String, String>();
    filterMap.put(ServerConstants.STREAM_FILTER, ServerDataHelper.getInstance()
        .getStreamFromGraphDataReq(filterValues));
    filterMap.put(ServerConstants.CLUSTER_FILTER, ServerDataHelper.getInstance()
        .getColoFromGraphDataReq(filterValues));
    filterMap
        .put(ServerConstants.START_TIME_FILTER, ServerDataHelper.getInstance()
            .getStartTimeFromGraphDataReq(filterValues));
    filterMap
        .put(ServerConstants.END_TIME_FILTER, ServerDataHelper.getInstance()
            .getEndTimeFromGraphDataReq(filterValues));
    return filterMap;
  }

  protected void createNode(Tuple tuple, Map<NodeKey, Node> nodeMap,
                          Set<Float> percentileSet,
                          Map<Float, Integer> tuplePercentileMap) {
    LOG.info("Creating node from tuple :" + tuple);
    String name, hostname = null;
    if (tuple.getTier().equalsIgnoreCase(Tier.HDFS.toString())) {
      name = tuple.getCluster();
      hostname = tuple.getHostname();
    } else {
      name = tuple.getHostname();
    }
    MessageStats receivedMessageStat =
        new MessageStats(tuple.getTopic(), tuple.getReceived(), hostname);
    MessageStats sentMessageStat =
        new MessageStats(tuple.getTopic(), tuple.getSent(), hostname);
    NodeKey newNodeKey = new NodeKey(name, tuple.getCluster(), tuple.getTier());
    List<MessageStats> receivedMessageStatsList = new ArrayList<MessageStats>(),
        sentMessageStatsList = new ArrayList<MessageStats>();
    receivedMessageStatsList.add(receivedMessageStat);
    sentMessageStatsList.add(sentMessageStat);
    Node node = nodeMap.get(newNodeKey);
    if (node == null) {
      node = new Node(name, tuple.getCluster(), tuple.getTier());
    }
    if (node.getReceivedMessagesList().size() > 0) {
      receivedMessageStatsList.addAll(node.getReceivedMessagesList());
    }
    if (node.getSentMessagesList().size() > 0) {
      sentMessageStatsList.addAll(node.getSentMessagesList());
    }
    node.setReceivedMessagesList(receivedMessageStatsList);
    node.setSentMessagesList(sentMessageStatsList);
    node.setPercentileSet(percentileSet);
    node.addToTopicPercentileMap(tuple.getTopic(), tuplePercentileMap);
    node.addToTopicCountMap(tuple.getTopic(), tuple.getLatencyCountMap());
    nodeMap.put(newNodeKey, node);
    LOG.debug("Node created: " + node);
  }

  private Map<Tuple, Map<Float, Integer>> getTierLatencyMap(String feederPath,
                                                            String endTime,
                                                            String startTime,
                                                            String
                                                                filterString) {
    ClientConfig config;
    if (feederPath == null || feederPath.length() == 0) {
      config =
          ClientConfig.load(ServerConstants.FEEDER_PROPERTIES_DEFAULT_PATH);
    } else {
      config = ClientConfig.load(feederPath);
    }
    AuditDbQuery dbQuery = new AuditDbQuery(endTime, startTime, filterString,
        "TIER", ServerConstants.TIMEZONE, properties.get(ServerConstants
        .PERCENTILE_FOR_SLA), config);
    try {
      dbQuery.execute();
    } catch (Exception e) {
      LOG.error("Exception while executing query: ", e);
    }
    LOG.info("Audit query: " + dbQuery.toString());
    try {
      dbQuery.displayResults();
    } catch (Exception e) {
      LOG.error("Exception while displaying results: ", e);
    }
    return dbQuery.getPercentile();
  }

  protected String setFilterString(Map<String, String> filterMap) {
    String filterString;
    String selectedStream = filterMap.get(ServerConstants.STREAM_FILTER);
    String selectedCluster = filterMap.get(ServerConstants.CLUSTER_FILTER);
    if (selectedStream.compareTo("All") == 0) {
      filterString = null;
    } else {
      filterString = "TOPIC=" + selectedStream;
    }
    if (!selectedCluster.equalsIgnoreCase("All")) {
      if (filterString == null || filterString.isEmpty()) {
        filterString = "CLUSTER=" + selectedCluster;
      } else {
        filterString += ",CLUSTER=" + selectedCluster;
      }
    }
    return filterString;
  }

  private void buildPercentileMapOfAllNodes(Map<NodeKey, Node> nodeMap) {
    for (Node node : nodeMap.values()) {
      node.buildPercentileMap();
    }
  }

  protected void addVIPNodesToNodesList(Map<NodeKey, Node> nodeMap,
                                      Set<Float> percentileSet) {
    Map<String, Node> vipNodeMap = new HashMap<String, Node>();
    for (Node node : nodeMap.values()) {
      if (node.getTier().equalsIgnoreCase(Tier.COLLECTOR.toString())) {
        Node vipNode = vipNodeMap.get(node.getClusterName());
        if (vipNode == null) {
          vipNode =
              new Node(node.getClusterName(), node.getClusterName(), "VIP");
          vipNode.setPercentileSet(percentileSet);
        }
        vipNode.setReceivedMessagesList(
            mergeLists(vipNode.getReceivedMessagesList(),
                node.getReceivedMessagesList()));
        vipNode.setSentMessagesList(mergeLists(vipNode.getSentMessagesList(),
            node.getSentMessagesList()));
        vipNode.addAllTopicCountMaps(node.getPerTopicCountMap());
        vipNodeMap.put(node.getClusterName(), vipNode);
      }
    }
    for (Map.Entry<String, Node> entry : vipNodeMap.entrySet()) {
      entry.getValue().buildPercentileMap();
      nodeMap.put(entry.getValue().getNodeKey(), entry.getValue());
    }
  }

  private List<MessageStats> mergeLists(List<MessageStats> list1,
                                        List<MessageStats> list2) {
    List<MessageStats> mergedList = new ArrayList<MessageStats>();
    if (list1.isEmpty() && !list2.isEmpty()) {
      for (MessageStats stats : list2) {
        mergedList.add(new MessageStats(stats));
      }
    } else if (!list1.isEmpty() && list2.isEmpty()) {
      for (MessageStats stats : list1) {
        mergedList.add(new MessageStats(stats));
      }
    } else {
      mergedList.addAll(list1);
      for (MessageStats stats : list2) {
        Long finalCount = stats.getMessages();
        boolean isPresent = false;
        for (MessageStats comparestats : mergedList) {
          if (comparestats.getTopic().equalsIgnoreCase(stats.getTopic())) {
            finalCount += comparestats.getMessages();
            comparestats.setMessages(finalCount);
            isPresent = true;
            break;
          }
        }
        if (!isPresent) {
          mergedList.add(new MessageStats(stats));
        }
      }
    }
    LOG.debug("List1: " + list1);
    LOG.debug("List2: " + list2);
    LOG.debug("MERGRED LIST : " + mergedList);
    return mergedList;
  }

  /**
   * If any node in the nodeList is a merge/mirror tier node, set the source
   * node's names list for merge/mirror node.
   *
   * @param nodeMap map of all nodeKey::nodes returned by the query
   */
  private void checkAndSetSourceListForMergeMirror(Map<NodeKey, Node> nodeMap) {
    for (Node node : nodeMap.values()) {
      if (node.getTier().equalsIgnoreCase("merge") ||
          node.getTier().equalsIgnoreCase("mirror")) {
        for (DatabusConfig config : dataBusConfig) {
          node.setSourceList(
              config.getClusters().get(node.getClusterName())
                  .getDestinationStreams().keySet());
        }
      }
    }
  }

  public List<DatabusConfig> getDataBusConfig() {
    return Collections.unmodifiableList(dataBusConfig);
  }
}