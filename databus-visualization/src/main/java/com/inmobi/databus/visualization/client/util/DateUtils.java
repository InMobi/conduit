package com.inmobi.databus.visualization.client.util;

import com.google.gwt.core.client.GWT;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.i18n.client.TimeZone;
import com.google.gwt.i18n.client.TimeZoneInfo;
import com.google.gwt.i18n.client.constants.TimeZoneConstants;
import com.google.gwt.user.datepicker.client.CalendarUtil;

import java.util.Date;

public class DateUtils {
  public static final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs
  public static final String AUDIT_DATE_FORMAT = "dd-MM-yyyy-HH:mm";
  public static final String BASE_DATE_FORMAT = "dd-MM-yyyy";
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

  public static String getCurrentTimeStringInGMT() {
    DateTimeFormat fmt = DateTimeFormat.getFormat("EEE MMM dd HH:mm:ss z " +
        "yyyy");
    Date currentDate = new Date();
    System.out.println("Curren date:"+currentDate.getTime());
    TimeZone tz = TimeZone.createTimeZone(0);
    return fmt.format(currentDate, tz);
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

  public static Date getDateFromAuditDateFormatString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    return formatter.parse(dateString);
  }

  public static String getBaseDateStringFromAuditDateFormat(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date date = formatter.parse(dateString);
    DateTimeFormat txtBoxFormatter =
        DateTimeFormat.getFormat(BASE_DATE_FORMAT);
    return txtBoxFormatter.format(date);
  }

  public static String getHourFromAuditDateFormatString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date date = formatter.parse(dateString);
    DateTimeFormat hourFormatter = DateTimeFormat.getFormat(HOUR_FORMAT);
    return hourFormatter.format(date);
  }

  public static String getMinuteFromAuditDateFormatString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    Date date = formatter.parse(dateString);
    DateTimeFormat minuteFormatter = DateTimeFormat.getFormat(MINUTE_FORMAT);
    return minuteFormatter.format(date);
  }

  public static String getPreviousDayString() {
    Date currentDate = new Date();
    CalendarUtil.addDaysToDate(currentDate, -1);
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    return formatter.format(currentDate);
  }

  public static String incrementAndGetTimeAsAuditDateFormatString(String currentTime,
                                           int numMinutes) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(AUDIT_DATE_FORMAT);
    long curTime = formatter.parse(currentTime).getTime() + numMinutes *
        ONE_MINUTE_IN_MILLIS;
    return formatter.format(new Date(curTime));
  }

  public static Date getDateFromBaseDateFormatString(String dateString) {
    DateTimeFormat formatter = DateTimeFormat.getFormat(BASE_DATE_FORMAT);
    return formatter.parse(dateString);
  }

  public static Date getDateWithZeroTime(Date date)
  {
    return DateTimeFormat.getFormat(BASE_DATE_FORMAT).parse(DateTimeFormat.getFormat
        (BASE_DATE_FORMAT).format(date));
  }

  public static Date getNextDay(Date date) {
    CalendarUtil.addDaysToDate(date, 1);
    return date;
  }

  public static Date getPreviousDay(Date date) {
    CalendarUtil.addDaysToDate(date, -1);
    return date;
  }
}
