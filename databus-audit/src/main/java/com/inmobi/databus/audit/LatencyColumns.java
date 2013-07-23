package com.inmobi.databus.audit;

/*
 * Order of the values defined here is important,always define the new values maintaining the ascending order
 */
public enum LatencyColumns {
  C0(0), C1(1), C2(2), C3(3), C4(4), C5(5), C6(6), C7(7), C8(8), C9(9),
  C10(10), C15(15), C30(30), C60(60), C120(120), C240(240), C600(600);

  private int value;

  private LatencyColumns(int value) {
    this.value = value;
  }

  public int getValue() {
    return this.value;
  }

  public static LatencyColumns getLatencyColumn(long latency){
    LatencyColumns[] columns = LatencyColumns.values();
    LatencyColumns current = columns[0];
    LatencyColumns next = null;
    for (int i = 1; i < columns.length; i++) {
      next = columns[i];
      if (latency >= current.getValue() * 60 * 1000
          && latency < next.getValue() * 60 * 1000) {
        return current;
      }
      current = next;
    }
    return C600;
  }
};
