package com.inmobi.databus.audit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AuditService implements Runnable {
  private Thread thread;
  private static final Log LOG = LogFactory.getLog(AuditService.class);
  @Override
  public void run() {
    execute();
  }

  public void start() {
    thread = new Thread(this, getServiceName());
    LOG.info("Starting thread " + thread.getName());
    thread.start();
  }

  public void join() {
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for thread " + thread.getName()
          + " to join", e);
    }
  }

  public abstract void stop();

  public abstract void execute();

  public abstract String getServiceName();
}
