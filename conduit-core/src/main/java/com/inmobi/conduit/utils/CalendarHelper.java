/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.inmobi.conduit.utils;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CalendarHelper {
  static Logger logger = Logger.getLogger(CalendarHelper.class);
  public static final String FILENAME_DELIMITER = "-";
  public static final String INDEX_DELIMITER = "_";

  static String minDirFormatStr = "yyyy" + File.separator + "MM" +
      File.separator + "dd" + File.separator + "HH" + File.separator +"mm";

  public static final ThreadLocal<DateFormat> minDirFormat =
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(minDirFormatStr);
    }
  };

  public static final ThreadLocal<DateFormat> collectorFileFormat =
      new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy" + "-" + "MM" + "-" + "dd" + "-"
          + "HH" + "-" + "mm");
    }
  };
  // TODO - all date/time should be returned in a common time zone GMT

  public static Date getDateFromStreamDir(Path streamDirPrefix, Path dir) {
    return getDateFromStreamDir(streamDirPrefix.toString(), dir.toString());
  }

  public static Date getDateFromStreamDir(String streamDirPrefix, String dir) {
    int startIndex = streamDirPrefix.length() + 1;
    String dirString = dir.substring(startIndex,
        startIndex + minDirFormatStr.length());
    try {
      return minDirFormat.get().parse(dirString);
    } catch (ParseException e) {
      logger.warn("Could not get date from directory passed", e);
    }
    return null;
  }

  public static Calendar getDate(String year, String month, String day) {
    return new GregorianCalendar(new Integer(year).intValue(), new Integer(
        month).intValue() - 1, new Integer(day).intValue());
  }

  public static Calendar getDate(Integer year, Integer month, Integer day) {
    return new GregorianCalendar(year.intValue(), month.intValue() - 1,
        day.intValue());
  }

  public static Calendar getDateHour(String year, String month, String day,
                                     String hour) {
    return new GregorianCalendar(new Integer(year).intValue(), new Integer(
        month).intValue() - 1, new Integer(day).intValue(),
        new Integer(hour).intValue(), new Integer(0));
  }

  public static Calendar getDateHourMinute(Integer year, Integer month,
                                           Integer day, Integer hour, Integer minute) {
    return new GregorianCalendar(year.intValue(), month.intValue() - 1,
        day.intValue(), hour.intValue(), minute.intValue());
  }

  public static String getCurrentMinute() {
    Calendar calendar;
    calendar = new GregorianCalendar();
    String minute = Integer.toString(calendar.get(Calendar.MINUTE));
    return minute;
  }

  public static String getCurrentHour() {
    Calendar calendar;
    calendar = new GregorianCalendar();
    String hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
    return hour;
  }

  public static Calendar getNowTime() {
    return new GregorianCalendar();
  }

  private static String getCurrentDayTimeAsString(boolean includeMinute) {
    return getDayTimeAsString(new GregorianCalendar(), includeMinute,
        includeMinute);
  }

  private static String getDayTimeAsString(Calendar calendar,
                                           boolean includeHour,
                                           boolean includeMinute) {
    String minute = null;
    String hour = null;
    String fileNameInnYYMMDDHRMNFormat = null;
    String year = Integer.toString(calendar.get(Calendar.YEAR));
    String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
    String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
    if (includeHour || includeMinute) {
      hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
    }
    if (includeMinute) {
      minute = Integer.toString(calendar.get(Calendar.MINUTE));
    }
    if (includeMinute) {
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day + "-" + hour
          + "-" + minute;
    } else if (includeHour) {
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day + "-" + hour;
    } else {
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day;
    }
    logger.debug("getCurrentDayTimeAsString ::  ["
        + fileNameInnYYMMDDHRMNFormat + "]");
    return fileNameInnYYMMDDHRMNFormat;

  }

  public static String getCurrentDayTimeAsString() {
    return getCurrentDayTimeAsString(true);
  }

  public static String getCurrentDayHourAsString() {
    return getDayTimeAsString(new GregorianCalendar(), true, false);
  }

  public static String getCurrentDateAsString() {
    return getCurrentDayTimeAsString(false);
  }

  public static String getDateAsString(Calendar calendar) {
    return getDayTimeAsString(calendar, false, false);
  }

  public static String getDateTimeAsString(Calendar calendar) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return format.format(calendar.getTime());
  }

  public static Calendar getDateTime(String dateTime) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    Calendar calendar = new GregorianCalendar();
    try {
      calendar.setTime(format.parse(dateTime));
    } catch(Exception e){
    }
    return calendar;
  }

  public static Path getPathFromDate(Date date, Path dirPrefix) {
    DateFormat dateFormat = new SimpleDateFormat(minDirFormatStr);
    String suffix = dateFormat.format(date);
    return new Path(dirPrefix, suffix);

  }

  public static Path getNextMinutePathFromDate(Date date, Path dirPrefix) {
    return getPathFromDate(addAMinute(date), dirPrefix);
  }

  public static Date addAMinute(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.MINUTE, 1);
    return calendar.getTime();
  }

  public static long getDateFromCollectorFileName(String collectorFileName) {
    String [] strs = collectorFileName.split("-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}");
    if (strs.length < 2) {
      logger.warn("Invalid file name: " + collectorFileName);
      return -1;
    }
    String streamName = strs[0];
    String dateSubStr = collectorFileName.substring(streamName.length() + 1);
    String[] str2 = dateSubStr.split("_");
    if (str2.length < 2) {
      logger.warn("Invalid file name:" + collectorFileName);
    }
    Date timestamp = null;
    try {
      timestamp =  collectorFileFormat.get().parse(str2[0]);
    } catch (ParseException e) {
      logger.warn("Invalid file name: " + collectorFileName);
      return -1;
    }
    return (timestamp == null) ? -1 : timestamp.getTime();
  }

}
