package com.inmobi.databus.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DatabusFileUtil {
  private static final Log LOG = LogFactory.getLog(DatabusFileUtil.class);

  /*
   * key is a combination of serviceName, stream, source cluster
   */
  public static String getSteamNameFromCheckPointKey(String key) {
    String[] st = key.split("_");
    if (st.length != 3) {
      LOG.error("Invalid checkpoint key " + key);
    } else {
      return st[1];
    }
    return null;
  }

  public static String getCheckPointKey(String serviceName, String stream,
      String source) {
    return serviceName + "_" + stream + "_" + source;
  }
}
