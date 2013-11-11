package com.inmobi.conduit.metrics;

import java.util.HashMap;
import java.util.Map;

import com.inmobi.databus.utils.DatabusFileUtil;

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
        streamName = DatabusFileUtil.getSteamNameFromCheckPointKey(key);
        pathToStreamCache.put(key, streamName);
      }
      return streamName;
    } catch (Exception ex) {
      return null;
    }
  }

}
