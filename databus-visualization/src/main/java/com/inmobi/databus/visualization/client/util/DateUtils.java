package com.inmobi.databus.visualization.client.util;

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.datepicker.client.CalendarUtil;

import java.util.Date;

public class DateUtils {
  public static final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs
  public static final String AUDIT_DATE_FORMAT = "dd-MM-yyyy-HH:mm";
  public static final String TEXTBOX_DATE_FORMAT = "dd-MM-yyyy";
  public static final String HOUR_FORMAT = "HH";
  public static final String MINUTE_FORMAT = "mm";

  /**
   * Checks if the date entered by user is of the format DD-MM-YYYY-HH:MN
   *
   * @param date Date as entered by user for audit query
   * @return true if date is of correct format else false
   */
  public static boolean checkTimeStringFormat(String date) {
    if (date.matches("[0-9]{2}-[0-9]{2}-[0-9]{4}-[0-9]{2}:[0-9]{2}$")) {
      String[] splits = date.split("-");
      if (splits.length != 4) {
        return false;
      }
      int day = Integer.parseInt(splits[0]);
      int month = Integer.parseInt(splits[1]);
      if (day < 1 || day > 31) {
        return false;
      }
      if (month < 1 || month > 12) {
        return false;
      }
      String[] time = splits[3].split(":");
      if (time.length != 2) {
        return false;
      }
      int hour = Integer.parseInt(time[0]);
      int minute = Integer.parseInt(time[1]);
      if (hour < 0 || hour > 24) {
        return false;
      }
      if (minute < 0 || minute > 60) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }

  public static String constructDateString(String date, String hour,
                                           String minute) {
    return date + "-" + hour + ":" + minute;
  }

  public static boolean checkIfFutureDate(String date) {
    DateTimeFormat fmt = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date parsedDate = fmt.parse(date);
    Date currentDate = new Date();
    return parsedDate.after(currentDate);
  }

  public static boolean checkStAfterEt(String start, String end) {
    DateTimeFormat fmt = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date startDate = fmt.parse(start);
    Date endDate = fmt.parse(end);
    return startDate.after(endDate);
  }

  public static Date getDateFromDateString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    return formatter.parse(dateString);
  }

  public static String getTextBoxValueFromDateString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date date = formatter.parse(dateString);
    DateTimeFormat txtBoxFormatter =
        DateTimeFormat.getFormat(TEXTBOX_DATE_FORMAT);
    return txtBoxFormatter.format(date);
  }

  public static String getHourFromDateString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date date = formatter.parse(dateString);
    DateTimeFormat hourFormatter = DateTimeFormat.getFormat(HOUR_FORMAT);
    return hourFormatter.format(date);
  }

  public static String getMinuteFromDateString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date date = formatter.parse(dateString);
    DateTimeFormat minuteFormatter = DateTimeFormat.getFormat(MINUTE_FORMAT);
    return minuteFormatter.format(date);
  }

  public static String getPreviousDayTime() {
    Date currentDate = new Date();
    CalendarUtil.addDaysToDate(currentDate, -1);
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    return formatter.format(currentDate);
  }

  public static String incrementAndGetTimeAsString(String currentTime,
                                           int numMinutes) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    long curTime = formatter.parse(currentTime).getTime() + numMinutes *
        ONE_MINUTE_IN_MILLIS;
    return formatter.format(new Date(curTime));
  }
}
