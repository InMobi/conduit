package com.inmobi.conduit.metrics;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Util class to get stream names from paths/keys
 */
public class MetricsUtil {

  private static final Log LOG = LogFactory.getLog(MetricsUtil.class);
  private static Map<String, String> pathToStreamCache = new HashMap<String, String>();

  /**
   * eg LocalStreamService_test1_col1 should give test1
   * 
   * @param path
   * @return
   */
  public static String getSteamNameFromCheckPointKey(String key) {
    try {
      String streamName = pathToStreamCache.get(key);
      if (streamName == null) {
        String[] st = key.split("_");
        if (st.length != 3) {
          LOG.error("Invalid checkpoint key " + key);
        } else {
          streamName = st[1];
        }
        pathToStreamCache.put(key, streamName);
      }
      return streamName;
    } catch (Exception ex) {
      return null;
    }
  }

}
