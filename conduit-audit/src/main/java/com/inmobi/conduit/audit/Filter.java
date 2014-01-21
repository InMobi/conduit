package com.inmobi.conduit.audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Filter {

  private Map<Column, List<String>> filters;

  public Filter(String input) {
    if (input == null) {
      filters = null;
    } else {
      filters = new HashMap<Column, List<String>>();
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
        // User can provide multiple options for each filtered column.
        // Values for the filtered column is separated by '|' symbol
        String[] values = stripQuotes(keyValues[1]).split("\\|");
        List<String> filterValues = new ArrayList<String>();
        for (int j = 0; j < values.length; j++) {
          filterValues.add(values[j]);
        }
        filters.put(key, filterValues);
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
      for (Entry<Column, List<String>> filter : filters.entrySet()) {
        if (!filter.getValue().contains(values.get(filter.getKey()))) {
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

  public Map<Column, List<String>> getFilters() {
    return filters;
  }
}
