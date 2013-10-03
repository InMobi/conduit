package com.conduit.metrics.impl.databus;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.MetricsService;

public class DatabusMetrics {
	Configuration config = null;
	public MetricsService mService;

	public DatabusMetrics() throws Exception {
		new MetricsService(config);
	}

	void init() {

		mService.addCounter("runtime");
		mService.addGuage("memory", new Long(100));

	}

	void update() {
		
		mService.incCounter("runtime");
		mService.updateGuage("memory", 10);
		//after some time
		

		mService.incCounter("runtime");
		mService.updateGuage("memory", 20);
		
		

		mService.incCounter("runtime");
		mService.updateGuage("memory", 50);
		

	}

}
