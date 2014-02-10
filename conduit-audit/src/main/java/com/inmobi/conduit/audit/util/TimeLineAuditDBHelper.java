package com.inmobi.conduit.audit.util;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.conduit.audit.Column;
import com.inmobi.conduit.audit.Filter;
import com.inmobi.conduit.audit.GroupBy;
import com.inmobi.conduit.audit.LatencyColumns;
import com.inmobi.messaging.ClientConfig;

public class TimeLineAuditDBHelper extends AuditDBHelper {

  private static final Log LOG = LogFactory.getLog(TimeLineAuditDBHelper.class);
  public static final String TIMEBUCKET = "visualization.timebucket";
  private int timeGroup;

  public TimeLineAuditDBHelper(ClientConfig config) {
    super(config);
    int timebucketConfig = config.getInteger(TIMEBUCKET, 60);
    timeGroup = timebucketConfig * 60000;
  }

  @Override
  public String getSelectStmtForRetrieve(Filter filter, GroupBy groupBy) {
    String sumString = "", whereString = "", groupByString = "", selectFieldString =
        "";
    for (LatencyColumns latencyColumn : LatencyColumns.values()) {
      sumString +=
          ", Sum(" + latencyColumn.toString() + ") as "
              + latencyColumn.toString();
    }
    if (filter.getFilters() != null) {
      for (Column column : Column.values()) {
        List<String> values = filter.getFilters().get(column);
        if (values != null && !values.isEmpty()) {
          whereString += " and (" + column.toString() + " = ?";
          for (int i = 1; i < values.size(); i++) {
            whereString += " or " + column.toString() + " = ?";
          }
          whereString += ")";
        }
      }
    }
    for (Column column : groupBy.getGroupByColumns()) {
      if (column.equals(Column.TIMEINTERVAL)) {
        groupByString += ", " + "(TIMEINTERVAL/" + timeGroup + ") ";
        selectFieldString +=
            ", " + "(TIMEINTERVAL/" + timeGroup + ") * " + timeGroup
                + " as TIMEINTERVAL";
      } else {
        groupByString += ", " + column.toString();
        selectFieldString += ", " + column.toString();
      }
    }
    String statement =
        "select Sum(" + AuditDBConstants.SENT + ") as " + AuditDBConstants.SENT
            + sumString + selectFieldString + " from " + getTableName() + " "
            + "where " + AuditDBConstants.TIMESTAMP + " >= ? and "
            + AuditDBConstants.TIMESTAMP + " < ? " + whereString;
    if (!groupByString.isEmpty()) {
      statement += " group by " + groupByString.substring(1);
      statement += " order by " + groupByString.substring(1);
    }
    LOG.debug("Select statement " + statement);
    return statement;
  }

}
