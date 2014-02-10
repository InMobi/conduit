package com.inmobi.conduit.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

public class TestConduitMetrics {

  @BeforeTest
  public void init() throws IOException {
    Properties prop = new Properties();
    prop.put("com.inmobi.conduit.metrics.enabled", "true");
    ConduitMetrics.init(prop);

  }

  @Test
  public void testRegisterAbsoluteGauge() {
    final String abGaugeName = "test.abs.guage";
    AbsoluteGauge abGauge = ConduitMetrics.registerAbsoluteGauge(abGaugeName, 5);
    Assert.assertNotNull(abGauge);
    Assert.assertEquals(abGauge.getValue(), 5);
    abGauge.setValue(2);
    Assert.assertEquals(abGauge.getValue(), 2);
    abGauge = ConduitMetrics.registerAbsoluteGauge(abGaugeName, 5);
    Assert.assertNull(abGauge);

  }
  /**
  @Test
  public void testRegisterCounter() {
    final String counterName = "test.counter";
    Counter abCounter = ConduitMetrics.registerCounter(counterName);
    Assert.assertNotNull(abCounter);
    abCounter = ConduitMetrics.getCounter(counterName);
    Assert.assertNotNull(abCounter);
    abCounter.inc();
    Assert.assertEquals(abCounter.getCount(), 1);
    abCounter.inc(4);
    Assert.assertEquals(abCounter.getCount(), 5);
    // trying to register again
    abCounter = ConduitMetrics.registerCounter(counterName);
    Assert.assertNull(abCounter);

  }*/

  @SuppressWarnings("unchecked")
  @Test
  public void testRegisterGauge() {
    final String gaugeName = "test.guage";
    final List<Integer> l = new ArrayList<Integer>();
    Gauge<Integer> gaugeInst = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return l.size();
      }
    };
    gaugeInst = ConduitMetrics.registerGauge(gaugeName, gaugeInst);
    Assert.assertNotNull(gaugeInst);
    Assert.assertEquals(gaugeInst.getValue(), new Integer(0));
    l.add(2);
    Assert.assertEquals(gaugeInst.getValue(), new Integer(1));
    gaugeInst = ConduitMetrics.registerGauge(gaugeName, gaugeInst);
    Assert.assertNull(gaugeInst);

  }


  @Test
  public void testRegisterCounter() {
    final String counterName = "test.counter";
    final String serviceName = "LocalStreamService";
    final String context = "stream1";
    Counter abCounter = ConduitMetrics.registerCounter(serviceName, counterName, context);
    Assert.assertNotNull(abCounter);
    abCounter = ConduitMetrics.getMetric(serviceName, counterName, context);
    Assert.assertNotNull(abCounter);
    abCounter.inc();
    Assert.assertEquals(abCounter.getCount(), 1);
    abCounter.inc(4);
    Assert.assertEquals(abCounter.getCount(), 5);
    //
    ConduitMetrics.incCounter(serviceName, counterName, context, 9);
    Assert.assertEquals(abCounter.getCount(), 14);
    // trying to register again
    abCounter = ConduitMetrics.registerCounter(serviceName, counterName, context);
    Assert.assertNull(abCounter);

  }
  
  
  @Test
  public void testSWGauge() {
    final String guageName = "test.sw.gauge";
    final String serviceName = "LocalStreamService";
    final String context = "stream1";
    SlidingTimeWindowGauge abGauge =
        ConduitMetrics.registerSlidingWindowGauge(serviceName, guageName,
            context);
    Assert.assertNotNull(abGauge);
    abGauge = ConduitMetrics.getMetric(serviceName, guageName, context);
    Assert.assertNotNull(abGauge);
    ConduitMetrics.updateSWGuage(serviceName, guageName, context, 1);
    Assert.assertEquals(abGauge.getValue().longValue(), 1);
    ConduitMetrics.updateSWGuage(serviceName, guageName, context, 4);
    Assert.assertEquals(abGauge.getValue().longValue(), 5);
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
    Assert.assertEquals(abGauge.getValue().longValue(), 0);
    ConduitMetrics.updateSWGuage(serviceName, guageName, context, 9);
    Assert.assertEquals(abGauge.getValue().longValue(), 9);
    // trying to register again
    abGauge =
        ConduitMetrics.registerSlidingWindowGauge(serviceName, guageName,
            context);
    Assert.assertNull(abGauge);

  }

}
