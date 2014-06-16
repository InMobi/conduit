package com.inmobi.conduit.metrics;

import com.codahale.metrics.Gauge;

/**
 * A Gauge which can set absolute values of a metric. 
 */
public class AbsoluteGauge implements Gauge {

  Number value;

  public AbsoluteGauge(Number value) {
    this.value = value;
  }

  public void setValue(Number value) {
    this.value = value;
  }

  public Number getValue() {
    return this.value;
  }
}
