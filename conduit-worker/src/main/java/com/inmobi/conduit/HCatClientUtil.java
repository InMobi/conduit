package com.inmobi.conduit;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;

public class HCatClientUtil {
  private static final Log LOG = LogFactory.getLog(HCatClientUtil.class);
  private String metastoreURL = null;
  protected BlockingQueue<HCatClient> buffer;

  public HCatClientUtil(String metastoreURL) {
    this.metastoreURL = metastoreURL;
  }

  public String getMetastoreUrl() {
    return metastoreURL; 
  }

  public void createHCatClients(int numOfHCatClients, HiveConf hcatConf)
      throws HCatException, InterruptedException {
    buffer = new LinkedBlockingDeque<HCatClient>(numOfHCatClients);
    for (int i = 0; i < numOfHCatClients; i++) {
      HCatClient hcatClient = HCatClient.create(hcatConf);
      buffer.put(hcatClient);
    }
    LOG.info("Total number of hcat clients are " + buffer.size());
  }

  public HCatClient getHCatClient() throws InterruptedException {
    if (buffer != null) {
      return buffer.poll(30, TimeUnit.SECONDS);
    } else {
      return null;
    }
  }

  public void addToPool(HCatClient hcatClient) {
    if (buffer != null) {
      buffer.offer(hcatClient);
    }
  }

  public void close() {
    Iterator<HCatClient> hcatIt = buffer.iterator();
    while(hcatIt.hasNext()) {
      try {
        hcatIt.next().close();
      } catch (HCatException e) {
        LOG.info("Exception occured while closing HCatClient ");
      }
    }
  }
}
