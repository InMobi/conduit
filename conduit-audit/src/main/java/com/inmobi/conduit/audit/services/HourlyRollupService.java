package com.inmobi.conduit.audit.services;

import com.inmobi.conduit.audit.util.AuditDBConstants;
import com.inmobi.conduit.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

public class HourlyRollupService extends AuditRollUpService {
  private int dailyTillDays;
  private static final Log LOG = LogFactory.getLog(HourlyRollupService.class);

  public HourlyRollupService(ClientConfig config) {
    super(config);
    rollupChkPtKey = config.getString(AuditDBConstants.CHECKPOINT_KEY,
        AuditDBConstants.DEFAULT_CHECKPOINT_KEY);
    if (rollupChkPtKey.length() == 0) {
      rollupChkPtKey = AuditDBConstants.DEFAULT_CHECKPOINT_KEY;
    }
    intervalLength = config.getLong(AuditDBConstants.INTERVAL_LENGTH_KEY,
        3600000l);
    dailyTillDays = config.getInteger(AuditDBConstants
        .DAILY_ROLLUP_TILLDAYS_KEY, tillDays + AuditDBConstants
        .DEFAULT_GAP_BTW_ROLLUP_TILLDAYS);
    LOG.info("Initialized " + getServiceName() + " with configs rollup hour " +
        "as: " + rollUpHourOfDay + ", interval length as:" + intervalLength +
        ", checkpoint directory as " + rollupChkPtDir + ", " +
        "checkpoint key as " + rollupChkPtKey + ", and till days conf as " +
        tillDays);
  }

  @Override
  public void executeRollup(Connection connection) throws SQLException {
    boolean isCreated = createDailyTable(connection);
    if (isCreated) {
      Date markTime = rollupTables(connection);
      if (markTime != null) {
        mark(AuditDBHelper.getFirstMilliOfDay(markTime));
      }
    }
  }

  @Override
  protected Date rollupTables(Connection connection) throws SQLException {
    if (!isStop) {
      Date fromTime = getRollupTime();
      Date toDate;
      if (fromTime.before(AuditDBHelper.addDaysToCurrentDate
          (-dailyTillDays))) {
        fromTime = AuditDBHelper.addDaysToCurrentDate(-dailyTillDays);
      }
      toDate = AuditDBHelper.addDaysToCurrentDate(-tillDays);
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
    return createMinuteTableName(config, date);
  }

  @Override
  public String getServiceName() {
    return "HourlyRollupService";
  }
}
