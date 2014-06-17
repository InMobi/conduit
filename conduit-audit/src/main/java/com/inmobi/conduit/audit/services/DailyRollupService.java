package com.inmobi.conduit.audit.services;

import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

public class DailyRollupService extends AuditRollUpService {
  private static final Log LOG = LogFactory.getLog(DailyRollupService.class);

  public DailyRollupService(ClientConfig config) {
    super(config);
    rollupChkPtKey = config.getString(AuditDBConstants.DAILY_ROLLUP_CHECKPOINT_KEY,
        AuditDBConstants.DEFAULT_DAILY_CHECKPOINT_KEY);
    if (rollupChkPtKey.length() == 0) {
      rollupChkPtKey = AuditDBConstants.DEFAULT_DAILY_CHECKPOINT_KEY;
    }
    intervalLength = 24 * 60 * 60 * 1000;
    int hourlyTilldays = config.getInteger(AuditDBConstants.TILLDAYS_KEY,
        AuditDBConstants.DEFAULT_HOURLY_ROLLUP_TILLDAYS);
    tillDays = config.getInteger(AuditDBConstants
        .DAILY_ROLLUP_TILLDAYS_KEY, hourlyTilldays + AuditDBConstants
        .DEFAULT_GAP_BTW_ROLLUP_TILLDAYS);
    if (tillDays <= hourlyTilldays) {
      LOG.error("Passed configs for daily.rollup.tilldays[" + tillDays +
          "] is less than rollup.tilldays[" + hourlyTilldays + "]");
      tillDays = hourlyTilldays + AuditDBConstants.DEFAULT_GAP_BTW_ROLLUP_TILLDAYS;
      LOG.info("Reset daily.rollup.tilldays to " + tillDays);
    }
    LOG.info("Initialized " + getServiceName() + " with configs rollup hour " +
        "as: " + rollUpHourOfDay + ", interval length as:" + intervalLength +
        ", checkpoint directory as " + rollupChkPtDir + ", " +
        "checkpoint key as " + rollupChkPtKey + ", and till days conf as " +
        tillDays);
  }

  @Override
  public void executeRollup(Connection connection) throws SQLException {
    Date markTime = rollupTables(connection);
    if (markTime != null) {
      mark(AuditDBHelper.getFirstMilliOfDay(markTime));
    }
  }

  @Override
  protected Date rollupTables(Connection connection) throws SQLException {
    if (!isStop) {
      Date fromTime = getRollupTime();
      Date toDate = AuditDBHelper.addDaysToCurrentDate(-tillDays);
      if (fromTime != null && !fromTime.after(toDate)) {
        return rollupTables(fromTime, toDate, connection);
      } else {
        LOG.error("Start time[" + fromTime +"] is after end time[" + toDate
            + "] for rollup or start time is null");
      }
    }
    return null;
  }

  @Override
  public String getSourceTable(Connection connection, Date date) {
    String srcTable;
    // Check if hourly table of currentDate exits,
    // if it dosen't then srcTable is minutely table
    String temp = createHourTableName(config, date);
    if (checkTableExists(connection, temp)) {
      srcTable = temp;
    } else {
      srcTable = createMinuteTableName(config, date);
    }
    return srcTable;
  }

  @Override
  public String createRolledTableNameForService(Date date) {
    return createDailyTableName(config, date);
  }

  @Override
  public String getServiceName() {
    return "DailyRollUpService";
  }
}
