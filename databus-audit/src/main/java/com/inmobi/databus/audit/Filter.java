package com.inmobi.databus.audit;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class Filter {

  private Map<Column, String> filters;

  public Filter(String input) {
    if (input == null) {
      filters = null;
    } else {
      filters = new HashMap<Column, String>();
      String inputSplit[] = input.split(",");
      for (int i = 0; i < inputSplit.length; i++) {
        String tmp = inputSplit[i];
        String keyValues[] = tmp.split("=");
        if (keyValues.length != 2) {
          continue; // skip this filter as it is malformed
        }
        Column key;
        try {
          key = Column.valueOf(keyValues[0].toUpperCase());
        } catch (Exception e) {
          continue;
        }
        String value = stripQuotes(keyValues[1]);
        filters.put(key, value);

      }
    }
  }

  private static String stripQuotes(String input) {
    if (input.startsWith("'") || input.startsWith("\"")) {
      input = input.substring(1);
    }
    if (input.endsWith("'") || input.endsWith("\"")) {
      input = input.substring(0, input.length() - 1);
    }
    return input;
  }

  public boolean apply(Map<Column, String> values) {

    if (filters != null) {
      for (Entry<Column, String> filter : filters.entrySet()) {
        if (!filter.getValue().equals(values.get(filter.getKey()))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "Filter" + filters;
  }

  public Map<Column, String> getFilters() {
    return filters;
  }
}
