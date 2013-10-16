package com.inmobi.conduit.metrics;

import java.io.IOException;
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
		prop.put("com.inmobi.databus.metrics.enabled", "true");
		ConduitMetrics.init(prop);

	}
	
	@Test
	public void testRegisterAbsoluteGauge() {
		AbsoluteGauge abGuage = ConduitMetrics.registerAbsoluteGauge("test.abs.guage", 5);
		Assert.assertNotNull(abGuage);
		abGuage = ConduitMetrics.registerAbsoluteGauge("test.abs.guage", 5);
		Assert.assertNull(abGuage);

	}

	@Test
	public void testRegisterCounter() {
		Counter abGuage = ConduitMetrics.registerCounter("test.counter");
		Assert.assertNotNull(abGuage);
		abGuage = ConduitMetrics.registerCounter("test.counter");
		Assert.assertNull(abGuage);
		abGuage = ConduitMetrics.getCounter("test.counter");
		Assert.assertNotNull(abGuage);

	}

	@Test
	public void testRegisterGauge() {
		Gauge g = new Gauge<Long>() {
			@Override
			public Long getValue() {
				return 1l;
			}
		};
		g = ConduitMetrics.registerGauge("test.guage", g);
		Assert.assertNotNull(g);
		g = ConduitMetrics.registerGauge("test.guage", g);
		Assert.assertNull(g);

	}

}
