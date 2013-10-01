package com.conduit.metrics;

import org.apache.commons.configuration.Configuration;

import com.conduit.metrics.guage.Counter;
import com.conduit.metrics.guage.Guage;
import com.conduit.metrics.guage.GuagueFactory;

public class DatabusMetrics extends AbstractMetrics {

	private Guage<Long> timeGuage;
	private Counter memoryCounter;

	@Override
	void init(Configuration config) throws Exception {
		super.init(config);

		timeGuage = GuagueFactory.createLongGuage(getType(), "time", this.getConfig(), new Long(0));
		memoryCounter = GuagueFactory.createCounter(getType(), "memory", this.getConfig());

	}

	public DatabusMetrics(Configuration config) {
		super(config);

	}

	public void setTime(long time) {
		this.timeGuage.setValue(time);
	}

	public void incMemory() {
		memoryCounter.inc();
	}

}
