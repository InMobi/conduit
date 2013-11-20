package com.inmobi.databus.audit;

import com.inmobi.databus.audit.services.AuditRollUpService;
import com.inmobi.databus.audit.util.AuditDBConstants;
import com.inmobi.databus.audit.util.AuditDBHelper;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.util.AuditUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class AuditAdmin {
  private static int minArgs = 3;
  private static final SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");

  public static void main(String[] args) {
    System.out.println("\n");
    if (args.length < minArgs) {
      System.out.println("Insufficient number of arguments");
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
    } else if (args[0].equals("-check")) {
      if (args[1].equals("-rolledup")) {
        run = 4;
      } else if (args[1].equals("-created")) {
        run = 5;
      }
    } else {
      printUsage();
      System.exit(-1);
    }

    int numDays = 1;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-date")) {
        date = args[i+1];
      } else if (args[i].equals("-n")) {
        numDays = Integer.parseInt(args[i+1]);
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
          isSuccess = rollupDayTable(rollUpService, currentDate, numDays,
              config);
          break;
        case 2:
          isSuccess = createDayTable(rollUpService, currentDate,
              numDays, config);
          break;
        case 3:
          isSuccess = checkpointRollupToDate(rollUpService, currentDate);
          break;
        case 4:
          isSuccess = checkTableExists(rollUpService, currentDate, numDays,
              config, true);
          break;
        case 5:
          isSuccess = checkTableExists(rollUpService, currentDate, numDays,
              config, false);
          break;
        default:
          System.out.println("Invalid run option");
          isSuccess = false;
      }
    } catch (SQLException e) {
      AuditDBHelper.logNextException("Exception thrown", e);
      isSuccess = false;
    }

    if (isSuccess) {
      System.out.println("---------");
      System.out.println(" SUCCESS ");
      System.out.println("---------");
    } else {
      System.out.println("------");
      System.out.println(" FAIL ");
      System.out.println("------");
    }
  }

  private static boolean checkTableExists(AuditRollUpService service,
                                       Date date, int numDays,
                                       ClientConfig config,
                                       boolean isRolledUp) {
    Connection connection = null;
    boolean isSuccess = true;
    try {
      connection = AuditDBHelper.getConnection(config.getString
          (AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
          config.getString(AuditDBConstants.DB_URL),
          config.getString(AuditDBConstants.DB_USERNAME),
          config.getString(AuditDBConstants.DB_PASSWORD));
      for (int i = 0; i < numDays; i++) {
        Date currentDate = service.addDaysToGivenDate(date, i);
        String tableName = service.createTableName(currentDate, isRolledUp);
        boolean isExists = service.checkTableExists(connection, tableName);
        if (!isExists) {
          isSuccess = false;
          if (isRolledUp) {
            System.out.println("Table not rolled up for date: " +
                formatter.format(currentDate));
          } else {
            System.out.println("Table not created for date: " +
                formatter.format(currentDate));
          }
        }
      }
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return isSuccess;
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
                                        Date date, int numDays,
                                        ClientConfig config) {
    Connection connection = null;
    try {
      connection = AuditDBHelper.getConnection(config.getString
          (AuditDBConstants.JDBC_DRIVER_CLASS_NAME), 
          config.getString(AuditDBConstants.DB_URL), 
          config.getString(AuditDBConstants.DB_USERNAME), 
          config.getString(AuditDBConstants.DB_PASSWORD));
      Date toDate = service.addDaysToGivenDate(date, numDays - 1);
      return service.createDailyTable(date, toDate, connection);
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
                                        Date fromDate, int numDays,
                                        ClientConfig config) throws SQLException{
    Connection connection = null;
    try {
      connection = AuditDBHelper.getConnection(config.getString
          (AuditDBConstants.JDBC_DRIVER_CLASS_NAME),
          config.getString(AuditDBConstants.DB_URL),
          config.getString(AuditDBConstants.DB_USERNAME),
          config.getString(AuditDBConstants.DB_PASSWORD));
      Date upperLimitDate = service.addDaysToCurrentDate(-1 * config.getInteger
          (AuditDBConstants.TILLDAYS_KEY));
      if (!fromDate.before(upperLimitDate)) {
        System.out.println("Incorrect day passed: day after rollup upper " +
            "limit i.e " + formatter.format(upperLimitDate));
        return false;
      }
      Date toDate = service.addDaysToGivenDate(fromDate, numDays);
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
    System.out.println("Usage:");
    System.out.println("-rollup -date <dd-mm-yyyy> [-n <number of days>] --conf <conf path>");
    System.out.println("-create -date <dd-mm-yyyy> [-n <number of days>] --conf <conf path>");
    System.out.println("-checkpoint -date <dd-mm-yyyy> --conf <conf path>");
    System.out.println("-check -rolledup -date <dd-mm-yyyy> [-n <number of days to check>] --conf <conf path>");
    System.out.println("-check -created -date <dd-mm-yyyy> [-n <number of days to check>] --conf <conf path>");
  }
}