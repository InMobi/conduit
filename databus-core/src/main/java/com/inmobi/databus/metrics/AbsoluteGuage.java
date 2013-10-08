package com.inmobi.databus.metrics;

public class AbsoluteGuage {

	Number value;

	public AbsoluteGuage(Number value) {
		this.value = value;

	}

	public void setValue(Number value) {
		this.value = value;
	};

	public Number getValue() {
		return this.value;
	}
}
