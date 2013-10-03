package com.conduit.metrics.guage.met.impl;

import com.conduit.metrics.guage.Guage;

public class CodahaleGuage implements Guage {

	Number value;

	public CodahaleGuage(Number value) {
		this.value = value;

	}

	public void setValue(Number value) {

		this.value = value;
	};

	@Override
	public Number getValue() {

		return this.value;
	}
}
