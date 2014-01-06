package com.inmobi.conduit.visualization.client.util;

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.i18n.client.TimeZone;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.datepicker.client.CalendarUtil;

import java.util.Date;

public class DateUtils {
  public static final long ONE_HOUR_IN_MINS = 60;
  public static final long ONE_MINUTE_IN_MILLIS = 60000;//millisecs
  public static final DateTimeFormat GMT_STRING_FORMATTER = DateTimeFormat
      .getFormat("EEE MMM dd HH:mm:ss z yyyy");
  public static final DateTimeFormat BASE_DATE_FORMATTER =
      DateTimeFormat.getFormat("dd-MM-yyyy");
  public static final DateTimeFormat AUDIT_DATE_FORMATTER =
      DateTimeFormat.getFormat("dd-MM-yyyy-HH:mm");
  public static final DateTimeFormat HOUR_FORMATTER =
      DateTimeFormat.getFormat("HH");
  public static final DateTimeFormat MINUTER_FORMATTER =
      DateTimeFormat.getFormat("mm");

  public static String constructDateString(String date, String hour,
                                           String minute) {
    return date + "-" + hour + ":" + minute;
  }

  public static String getCurrentTimeStringInGMT() {
    Date currentDate = new Date();
    TimeZone tz = TimeZone.createTimeZone(0);
    return GMT_STRING_FORMATTER.format(currentDate, tz);
  }

  public static boolean checkIfFutureDate(String date) {
    Date parsedDate = AUDIT_DATE_FORMATTER.parse(date);
    Date currentDate = new Date();
    int tz = parsedDate.getTimezoneOffset();
    Date newParsedDate = new Date(parsedDate.getTime()
        -tz*ONE_MINUTE_IN_MILLIS);
    return newParsedDate.getTime() >= currentDate.getTime();
  }

  public static boolean checkStAfterEt(String start, String end) {
    Date startDate = AUDIT_DATE_FORMATTER.parse(start);
    Date endDate = AUDIT_DATE_FORMATTER.parse(end);
    return startDate.after(endDate);
  }

  public static Date getDateFromAuditDateFormatString(String dateString) {
    return AUDIT_DATE_FORMATTER.parse(dateString);
  }

  public static String getBaseDateStringFromAuditDateFormat(String dateString) {
    Date date = AUDIT_DATE_FORMATTER.parse(dateString);
    return AUDIT_DATE_FORMATTER.format(date);
  }

  public static String getHourFromAuditDateFormatString(String dateString) {
    Date date = AUDIT_DATE_FORMATTER.parse(dateString);
    return HOUR_FORMATTER.format(date);
  }

  public static String getMinuteFromAuditDateFormatString(String dateString) {
    Date date = AUDIT_DATE_FORMATTER.parse(dateString);
    return MINUTER_FORMATTER.format(date);
  }

  public static String getPreviousDayString() {
    Date currentDate = new Date();
    CalendarUtil.addDaysToDate(currentDate, -1);
    return AUDIT_DATE_FORMATTER.format(currentDate);
  }

  public static String incrementAndGetTimeAsAuditDateFormatString(String currentTime,
                                           int numMinutes) {
    long curTime = AUDIT_DATE_FORMATTER.parse(currentTime).getTime() + numMinutes *
        ONE_MINUTE_IN_MILLIS;
    return AUDIT_DATE_FORMATTER.format(new Date(curTime));
  }

  public static Date getDateFromBaseDateFormatString(String dateString) {
    return BASE_DATE_FORMATTER.parse(dateString);
  }

  public static Date getDateWithZeroTime(Date date) {
    return BASE_DATE_FORMATTER.parse(BASE_DATE_FORMATTER.format(date));
  }

  public static Date getNextDay(Date date) {
    return getDateByOffset(date, 1);
  }

  public static Date getPreviousDay(Date date) {
    return getDateByOffset(date, -1);
  }

  public static boolean checkTimeInterval(String stTime, String edTime,
                                          String maxTimeInt) {
    Date start = AUDIT_DATE_FORMATTER.parse(stTime);
    Date end = AUDIT_DATE_FORMATTER.parse(edTime);
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
      date = AUDIT_DATE_FORMATTER.parse(selectedDate);
    } else {
      date = BASE_DATE_FORMATTER.parse(selectedDate);
    }
    return checkSelectedDateRolledUp(date, rolledUpTillDayas);
  }

  public static boolean checkSelectedDateRolledUp(Date selectedDate, int rolledUpTillDayas) {
    String selectedDateStr = BASE_DATE_FORMATTER.format(selectedDate);
    Date currentDate = new Date();
    Date rolledUpDate = getDateByOffset(currentDate, -rolledUpTillDayas);
    String rolledUpDateStr = BASE_DATE_FORMATTER.format(rolledUpDate);
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
