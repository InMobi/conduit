package com.inmobi.databus.audit;

import com.inmobi.databus.audit.services.AuditRollUpService;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.util.AuditUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AuditAdmin {
  private static int minArgs = 3;
  private static final SimpleDateFormat formatter = new SimpleDateFormat(AuditUtil
      .DATE_FORMAT);

  public static void main(String[] args) {
    if (args.length < minArgs) {
      System.out.println("\nInsufficient number of arguments");
      printUsage();
      System.exit(-1);
    }

    int run = 0;
    String date = null;
    if (args[0].equals("-rollup")) {
      run = 1;
    } else if (args[0].equals("-create")) {
      run = 2;
    } else if (args[0].equals("-checkpoint")) {
      run = 3;
    } else {
      printUsage();
      System.exit(-1);
    }

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-date")) {
        date = args[i+1];
        break;
      }
    }

    Date currentDate = getDate(date);
    if (currentDate == null) {
      printUsage();
      System.exit(-1);
    }

    ClientConfig config = ClientConfig.loadFromClasspath(AuditDBConstants
        .FEEDER_CONF_FILE);
    AuditRollUpService rollUpService = new AuditRollUpService(config);

    boolean isSuccess;
    try {
      switch(run) {
        case 1:
          isSuccess = rollupDayTable(rollUpService, currentDate, config);
          break;
        case 2:
          isSuccess = createDayTable(rollUpService, currentDate, config);
          break;
        case 3:
          isSuccess = checkpointRollupToDate(rollUpService, currentDate);
          break;
        default:
          System.out.println("Invalid run option");
          isSuccess = false;
      }
    } catch (Exception e) {
      e.printStackTrace();
      isSuccess = false;
    }

    if (isSuccess) {
      System.out.println("SUCCESS");
    } else {
      System.out.println("FAILURE");
    }
  }

  private static boolean checkpointRollupToDate(AuditRollUpService service,
                                                Date date) {
    try {
      service.mark(date.getTime());
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  private static boolean createDayTable(AuditRollUpService service,
                                     Date date, ClientConfig config) {
    Connection connection = null;
    try {
      connection = AuditDBHelper.getConnection(config.getString
          (AuditDBConstants.JDBC_DRIVER_CLASS_NAME), 
          config.getString(AuditDBConstants.DB_URL), 
          config.getString(AuditDBConstants.DB_USERNAME), 
          config.getString(AuditDBConstants.DB_PASSWORD));
      return service.createDailyTable(date, date, connection);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static boolean rollupDayTable(AuditRollUpService service,
                                        Date fromDate, ClientConfig config) throws SQLException{
    Connection connection = null;
    try {
      connection = AuditDBHelper.getConnection(config.getString
          (AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
          config.getString(AuditDBConstants.DB_URL),
          config.getString(AuditDBConstants.DB_USERNAME),
          config.getString(AuditDBConstants.DB_PASSWORD));
      Date toDate = service.addDaysToGivenDate(fromDate, 1);
      System.out.println("To date:"+toDate);
      Date date = service.rollupTables(fromDate, toDate, connection);
      if (date.equals(fromDate))
        return false;
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return true;
  }

  private static Date getDate(String date) {
    Date currentDate;
    try {
      currentDate = formatter.parse(date);
      //passed date should not be before 01-01-2013-00:00
      if (currentDate.before(new Date(1356998400000l))) {
        return null;
      }
    } catch (Exception e) {
      return null;
    }
    return currentDate;
  }

  private static void printUsage() {
    System.out.println("\nUsage:");
    System.out.println("-rollup -date <dd-mm-yyyy-HH:mn> --conf <conf " +
        "path>");
    System.out.println("-create -date <dd-mm-yyyy-HH:mn> --conf <conf " +
        "path>");
    System.out.println("-checkpoint -date <dd-mm-yyyy-HH:mn> --conf <conf " +
        "path>");
  }
}