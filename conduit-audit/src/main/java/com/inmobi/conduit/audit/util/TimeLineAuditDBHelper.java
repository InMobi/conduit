package com.inmobi.conduit.audit.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.inmobi.messaging.util.AuditUtil;
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
  private static final long ONE_DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
  private static final long THREE_DAY_LIMIT = 3 * ONE_DAY_IN_MILLISECONDS;
  private static final long WEEK_LIMIT = 7 * ONE_DAY_IN_MILLISECONDS;
  private static final long TWO_WEEK_LIMIT = 14 * ONE_DAY_IN_MILLISECONDS;
  private static final long THREE_WEEK_LIMIT = 21 * ONE_DAY_IN_MILLISECONDS;
  private static final long MONTH_LIMIT = 30 * ONE_DAY_IN_MILLISECONDS;
  private static final long TWO_MONTH_LIMIT = 2 * 30 * ONE_DAY_IN_MILLISECONDS;
  public static final SimpleDateFormat auditDateFormatter = new
      SimpleDateFormat(AuditUtil.DATE_FORMAT);
  private int timeGroup;

  public TimeLineAuditDBHelper(ClientConfig config, String startTime,
                               String endTime) {
    super(config);
    int timebucketConfig = config.getInteger(TIMEBUCKET, 60);
    setTimeGroup(timebucketConfig, startTime, endTime);
  }

  private void setTimeGroup(int timebucketConfig, String startTime,
                            String endTime) {
    if (startTime == null || endTime == null) {
      timeGroup = timebucketConfig * 60000;
    }
    Date startDate, endDate;
    try {
      startDate = auditDateFormatter.parse(startTime);
      endDate = auditDateFormatter.parse(endTime);
      long rangeDiff = endDate.getTime() - startDate.getTime();
      int optimalTimeBucket;
      if (rangeDiff <= THREE_DAY_LIMIT) {
        optimalTimeBucket = 60;
      } else if (rangeDiff > THREE_DAY_LIMIT && rangeDiff <= WEEK_LIMIT) {
        optimalTimeBucket = 2 * 60;
      } else if (rangeDiff > WEEK_LIMIT && rangeDiff <= TWO_WEEK_LIMIT) {
        optimalTimeBucket = 4 * 60;
      } else if (rangeDiff > TWO_WEEK_LIMIT && rangeDiff <= THREE_WEEK_LIMIT) {
        optimalTimeBucket = 6 * 60;
      } else if (rangeDiff > THREE_WEEK_LIMIT && rangeDiff <= MONTH_LIMIT) {
        optimalTimeBucket = 8 * 60;
      } else if (rangeDiff > MONTH_LIMIT && rangeDiff <= TWO_MONTH_LIMIT) {
        optimalTimeBucket = 12 * 60;
      } else {
        optimalTimeBucket = 24 * 60;
      }
      timeGroup = ((timebucketConfig < optimalTimeBucket) ? optimalTimeBucket :
          timebucketConfig) * 60000;
    } catch (ParseException e) {
      e.printStackTrace();
    }
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

  public int getTimeGroup() {
    return timeGroup;
  }

}
