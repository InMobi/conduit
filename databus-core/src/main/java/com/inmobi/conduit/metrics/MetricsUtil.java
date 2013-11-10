package com.inmobi.conduit.metrics;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Util class to get stream names from paths/keys
 */
public class MetricsUtil {

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
        StringTokenizer st = new StringTokenizer(key, "_");
        st.nextToken();
        streamName = st.nextToken();
        pathToStreamCache.put(key, streamName);
      }
      return streamName;
    } catch (Exception ex) {
      return null;
    }
  }

}
