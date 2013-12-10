package com.inmobi.databus.visualization.client.util;

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.i18n.client.TimeZone;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.datepicker.client.CalendarUtil;

import java.util.Date;

public class DateUtils {
  public static final long ONE_HOUR_IN_MINS = 60;
  public static final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs
  public static final DateTimeFormat gmtStringFormatter = DateTimeFormat
      .getFormat("EEE MMM dd HH:mm:ss z yyyy");
  public static final DateTimeFormat baseDateFormatter =
      DateTimeFormat.getFormat("dd-MM-yyyy");
  public static final DateTimeFormat auditDateFormatter =
      DateTimeFormat.getFormat("dd-MM-yyyy-HH:mm");
  public static final DateTimeFormat hourFormatter =
      DateTimeFormat.getFormat("HH");
  public static final DateTimeFormat minuteFormatter =
      DateTimeFormat.getFormat("mm");

  public static String constructDateString(String date, String hour,
                                           String minute) {
    return date + "-" + hour + ":" + minute;
  }

  public static String getCurrentTimeStringInGMT() {
    Date currentDate = new Date();
    TimeZone tz = TimeZone.createTimeZone(0);
    return gmtStringFormatter.format(currentDate, tz);
  }

  public static boolean checkIfFutureDate(String date) {
    Date parsedDate = auditDateFormatter.parse(date);
    Date currentDate = new Date();
    int tz = parsedDate.getTimezoneOffset();
    Date newParsedDate = new Date(parsedDate.getTime()
        -tz*ONE_MINUTE_IN_MILLIS);
    return newParsedDate.getTime() >= currentDate.getTime();
  }

  public static boolean checkStAfterEt(String start, String end) {
    Date startDate = auditDateFormatter.parse(start);
    Date endDate = auditDateFormatter.parse(end);
    return startDate.after(endDate);
  }

  public static Date getDateFromAuditDateFormatString(String dateString) {
    return auditDateFormatter.parse(dateString);
  }

  public static String getBaseDateStringFromAuditDateFormat(String dateString) {
    Date date = auditDateFormatter.parse(dateString);
    return baseDateFormatter.format(date);
  }

  public static String getHourFromAuditDateFormatString(String dateString) {
    Date date = auditDateFormatter.parse(dateString);
    return hourFormatter.format(date);
  }

  public static String getMinuteFromAuditDateFormatString(String dateString) {
    Date date = auditDateFormatter.parse(dateString);
    return minuteFormatter.format(date);
  }

  public static String getPreviousDayString() {
    Date currentDate = new Date();
    CalendarUtil.addDaysToDate(currentDate, -1);
    return auditDateFormatter.format(currentDate);
  }

  public static String incrementAndGetTimeAsAuditDateFormatString(String currentTime,
                                           int numMinutes) {
    long curTime = auditDateFormatter.parse(currentTime).getTime() + numMinutes *
        ONE_MINUTE_IN_MILLIS;
    return auditDateFormatter.format(new Date(curTime));
  }

  public static Date getDateFromBaseDateFormatString(String dateString) {
    return baseDateFormatter.parse(dateString);
  }

  public static Date getDateWithZeroTime(Date date) {
    return baseDateFormatter.parse(baseDateFormatter.format(date));
  }

  public static Date getNextDay(Date date) {
    return getDateByOffset(date, 1);
  }

  public static Date getPreviousDay(Date date) {
    return getDateByOffset(date, -1);
  }

  public static boolean checkTimeInterval(String stTime, String edTime,
                                          String maxTimeInt) {
    Date start = auditDateFormatter.parse(stTime);
    Date end = auditDateFormatter.parse(edTime);
    Integer maxInt = Integer.parseInt(maxTimeInt);
    if((end.getTime() - start.getTime()) > (maxInt * ONE_HOUR_IN_MINS *
        ONE_MINUTE_IN_MILLIS))
      return true;
    return false;
  }

  public static boolean checkSelectedDateRolledUp(String selectedDate,
                                                  int rolledUpTillDayas,
                                                  boolean isAudit) {
    Date date;
    if (isAudit) {
      date = auditDateFormatter.parse(selectedDate);
    } else {
      date = baseDateFormatter.parse(selectedDate);
    }
    return checkSelectedDateRolledUp(date, rolledUpTillDayas);
  }

  public static boolean checkSelectedDateRolledUp(Date selectedDate, int rolledUpTillDayas) {
    String selectedDateStr = baseDateFormatter.format(selectedDate);
    Date currentDate = new Date();
    Date rolledUpDate = getDateByOffset(currentDate, -rolledUpTillDayas);
    String rolledUpDateStr = baseDateFormatter.format(rolledUpDate);
    if (selectedDateStr.compareTo(rolledUpDateStr) >= 0) {
      return false;
    }
    return true;
  }

  private static Date getDateByOffset(Date date, int i) {
    CalendarUtil.addDaysToDate(date, i);
    return date;
  }
}
