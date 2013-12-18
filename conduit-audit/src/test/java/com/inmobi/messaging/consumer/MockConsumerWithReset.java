package com.inmobi.messaging.consumer;

import com.inmobi.instrumentation.AbstractMessagingClientStatsExposer;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.util.AuditUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MockConsumerWithReset extends AbstractMessageConsumer {
  private Map<String, BlockingQueue<Message>> source, backupSource;

  public void setSource(Map<String, BlockingQueue<Message>> source) {
    this.source = source;
    this.backupSource = new HashMap<String, BlockingQueue<Message>>();
  }

  @Override
  public boolean isMarkSupported() {
    return true;
  }

  @Override
  protected AbstractMessagingClientStatsExposer getMetricsImpl() {
    return new BaseMessageConsumerStatsExposer(topicName, consumerName);
  }

  @Override
  protected void doMark() throws IOException {
  }

  @Override
  protected void doReset() throws IOException {
    BlockingQueue<Message> fullQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> sourceQueue = source.get(topicName);
    BlockingQueue<Message> backupQueue = backupSource.get(topicName);
    if (backupQueue != null) {
      backupQueue.drainTo(fullQueue);
    }
    if (sourceQueue != null) {
      sourceQueue.drainTo(fullQueue);
    }
    source.put(topicName, fullQueue);
  }

  @Override
  protected Message getNext() throws InterruptedException, EndOfStreamException {
    BlockingQueue<Message> queue = source.get(topicName);
    if (queue == null)
      queue = new LinkedBlockingQueue<Message>();
    Message msg = queue.take();
    BlockingQueue<Message> topicBackupQueue = backupSource.get(topicName);
    if (topicBackupQueue == null)
      topicBackupQueue = new LinkedBlockingQueue<Message>();
    topicBackupQueue.add(msg);
    backupSource.put(topicName, topicBackupQueue);
    msg.set(AuditUtil.removeHeader(msg.getData().array()));
    return msg;
  }

  @Override
  public synchronized Message next() throws InterruptedException,
      EndOfStreamException {
    Message msg = getNext();
    return msg;
  }

  @Override
  public synchronized Message next(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    Message msg = getNext(timeout, timeunit);
    return msg;
  }

  @Override
  protected Message getNext(long timeout, TimeUnit timeunit)
      throws InterruptedException, EndOfStreamException {
    BlockingQueue<Message> queue = source.get(topicName);
    if (queue == null)
      queue = new LinkedBlockingQueue<Message>();
    Message msg = queue.poll(timeout, timeunit);
    if (msg == null)
      return null;
    BlockingQueue<Message> topicBackupQueue = backupSource.get(topicName);
    if (topicBackupQueue == null)
      topicBackupQueue = new LinkedBlockingQueue<Message>();
    topicBackupQueue.add(msg);
    backupSource.put(topicName, topicBackupQueue);
    msg.set(AuditUtil.removeHeader(msg.getData().array()));
    return msg;
  }
}
