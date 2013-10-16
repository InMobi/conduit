package com.inmobi.conduit.metrics;

/**
 * A Gauge which can set absolute values of a metric. 
 */
public class AbsoluteGauge {

	Number value;

	public AbsoluteGauge(Number value) {
		this.value = value;

	}

	public void setValue(Number value) {
		this.value = value;
	};

	public Number getValue() {
		return this.value;
	}
}
