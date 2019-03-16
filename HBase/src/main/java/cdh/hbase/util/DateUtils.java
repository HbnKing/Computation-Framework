package cdh.hbase.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * 时间处理工具
 *
 * @author crush_lee
 */
public final class DateUtils
{
  private DateUtils()
  {
    super();
  }

  private static final Log LOG = LogFactory.getLog(DateUtils.class);
  /**
   * 年月日
   */
  public static final String YMD = "yyyy-MM-dd";
  public static final String YYYYMMDD = "yyyyMMdd";
  public static final String DD = "dd";
  public static final String YYYYMM = "yyyyMM";
  public static final String HH = "HH";
  /**
   * 年月日时分秒
   */
  public static final String YMD_HMS = "yyyy-MM-dd HH:mm:ss";
  public static final String YMD_HMSZ = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String BIG = "big";
  public static final String SMALL = "small";
  public static final String SHORT_DATE_FORMAT_STR = "yyyy-MM-dd";
  public static final String LONG_DATE_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";
  public static final String MAX_LONG_DATE_FORMAT_STR = "yyyy-MM-dd HH:mm:ss SSS";
  public static final String HOUR_MIN_FORMAT_STR = "HH:mm";
  public static final String EARLY_TIME = "00:00:00 000";
  public static final String LATE_TIME = "23:59:59";
  public static final String EARER_IN_THE_DAY = "yyyy-MM-dd 00:00:00 000";
  public static final String LATE_IN_THE_DAY = "yyyy-MM-dd 23:59:59 999";
  public static final long DAY_LONG = 24 * 60 * 60 * 1000;
  public static final String YYYY_MM_DD = "yyyy-MM-dd";
  private static final String WEEKLY_DATE_PATTERN = "%d_%02d";
  public static final String DATE_PATEN_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

  public static final int ONE_DAY_SECONDS = 86400;

  public static final int ONE_HOURS_SECONDS = 3600;

  public static final int ONE_MINUTE_SECONDS = 60;

  public static final int ONE_SECOND_MILLS = 1000;

  /**
   * @return
   */
  public static String getWeek(Date date)
  {
    return getCurrentWeekYear(date) + "_" + getCurrentWeek(date);
  }

  /**
   * 获取当前周的年份. 即在2014年12月28日, 返回的周为2015, 因为其周取201501
   *
   * @return 当前周所有的年份
   */
  private static int getCurrentWeekYear(Date date)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.setFirstDayOfWeek(Calendar.MONDAY);

    int weekNum = calendar.get(Calendar.WEEK_OF_YEAR);
    int year = calendar.get(Calendar.YEAR);

    //use new year week num at the end of December
    if ((1 == weekNum) && (Calendar.DECEMBER == calendar.get(Calendar.MONTH))) {
      calendar.roll(Calendar.DAY_OF_MONTH, -7);
      if (calendar.get(Calendar.WEEK_OF_YEAR) > weekNum) {
        year++;
      }
    }
    return year;
  }

  /**
   * 获取当天日期格式
   *
   * @param format
   * @return
     */
  public static String getCurrent(String format)
  {
    return formatDate(new Date(), format);
  }

  /**
   * 获取当前周号
   *
   * @return 当前周号
   */
  private static int getCurrentWeek(Date date)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    return calendar.get(Calendar.WEEK_OF_YEAR);
  }

  /**
   * 得到某个日期在这一天中时间最早的日期对象
   *
   * @param date
   * @return
   * @throws ParseException
   */
  public static Date getEarlyInTheDay(Date date) throws ParseException
  {
    String dateString = new SimpleDateFormat(SHORT_DATE_FORMAT_STR).format(date) + " " + EARLY_TIME;
    return new SimpleDateFormat(LONG_DATE_FORMAT_STR).parse(dateString);
  }

  /**
   * 得到某天最早的时间
   *
   * @param date
   * @return
   */
  public static Date getFirstOfDay(Date date)
  {
    String dateString = new SimpleDateFormat(EARER_IN_THE_DAY).format(date);
    try {
      return new SimpleDateFormat(LONG_DATE_FORMAT_STR).parse(dateString);
    } catch (ParseException e) {
      return null;
    }
  }

  /**
   * 得到某个日期在这一天中时间最晚的日期对象
   *
   * @param date
   * @return
   * @throws ParseException
   */
  public static Date getLateInTheDay(Date date) throws ParseException
  {
    String dateString = new SimpleDateFormat(SHORT_DATE_FORMAT_STR).format(date) + " " + LATE_TIME;
    return new SimpleDateFormat(LONG_DATE_FORMAT_STR).parse(dateString);
  }

  /**
   * 增加时间的秒数
   *
   * @param date   要增加的日期
   * @param second 增加的时间（以秒为单位）
   * @return 增加时间后的日期
   */

  public static Date addSecond(Date date, int second)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.SECOND, second);
    return cal.getTime();
  }

  /**
   * 根据传入日期计算和当前日期的相差天数
   *
   * @param date
   * @return
   */
  public static long subtractNowDay(Date date)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    long dateTimeInMillis = calendar.getTimeInMillis();
    Calendar nowCalendar = Calendar.getInstance();
    nowCalendar.setTime(new Date());
    long nowTimeInMillis = nowCalendar.getTimeInMillis();
    return (nowTimeInMillis - dateTimeInMillis) / (24 * 60 * 60 * 1000);
  }

  /**
   * 获取结束日期与开始日期相差的秒数
   *
   * @param startDate 开始日期
   * @param endDate   结束日期
   * @return
   */
  public static int subtractSecond(Date startDate, Date endDate)
  {
    Calendar startCalendar = Calendar.getInstance();
    startCalendar.setTime(startDate);
    long startTimeInMillis = startCalendar.getTimeInMillis();
    Calendar endCalendar = Calendar.getInstance();
    endCalendar.setTime(endDate);
    long endTimeInMillis = endCalendar.getTimeInMillis();
    return (int) ((endTimeInMillis - startTimeInMillis) / 1000);

  }

  /*
  * 获取当前日志对应的月的最大天数
  * */
  public static int getMaxDay(Date date)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return cal.getActualMaximum(Calendar.DAY_OF_MONTH);
  }

  /**
   * 字符串按自定格式更新
   *
   * @param dateString
   * @param format
   * @return
   * @throws ParseException
   */
  public static Date parserStringToDate(String dateString, String format) throws ParseException
  {
    DateFormat dateFormat = new SimpleDateFormat(format);
    return dateFormat.parse(dateString);
  }

  public static Date parse(String dateString, String format)
  {
    Date date = null;
    try {
      date = parserStringToDate(dateString, format);
    } catch (Exception e) {
      LOG.error(e);
    }
    return date;
  }

  public static Date parse4ShortPattern(String dateString)
  {
    return parse(dateString, SHORT_DATE_FORMAT_STR);
  }

  public static int getDateDistance(Date fromDate, Date toDate) throws Exception
  {
    if (null == fromDate || null == toDate) {
      return -1;
    }
    if (fromDate.getTime() - toDate.getTime() > 0) {
      throw new IllegalArgumentException("-- fromDate must less than toDate");
    }
    Date fromDateTemp = parserStringToDate(formatDate4ShortPattern(fromDate), SHORT_DATE_FORMAT_STR);
    Date toDateTemp = parserStringToDate(formatDate4ShortPattern(toDate), SHORT_DATE_FORMAT_STR);
    //修改coverity
    if (null == fromDateTemp || null == toDateTemp) {
      return -1;
    }
    long between = toDateTemp.getTime() - fromDateTemp.getTime();
    return (int) (between / (1000 * 60 * 60 * 24) + 1);
  }

  /**
   * 日期加减
   *
   * @param date
   * @param dateInterval
   * @return
   */
  public static Date dateInterval(Date date, int dateInterval)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.DAY_OF_MONTH, dateInterval);
    return cal.getTime();
  }

  /**
   * 获取某天向前或者向后推num天的日期
   *
    * @return
   * @throws ParseException
   */
  public static String getDateBeforeOrAfter(String dayFormat, String day, int num, String resultFormat)
  {
    SimpleDateFormat df = new SimpleDateFormat(dayFormat);
    Date date;

    try {
      date = df.parse(day);
    } catch (Exception e) {
      return null;
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.DAY_OF_MONTH, num);

    return new SimpleDateFormat(resultFormat).format(cal.getTime());
  }

  /**
   * 获取向前或向后的月
   *
   * @return
   * @throws ParseException
   */
  public static String getMonthBeforeOrAfter(String monthFormat, String month, int num, String resultFormat)
  {
    SimpleDateFormat df = new SimpleDateFormat(monthFormat);
    Date date;

    try {
      date = df.parse(month);
    } catch (Exception e) {
      return null;
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(Calendar.MONTH, num);

    return new SimpleDateFormat(resultFormat).format(cal.getTime());
  }

  /**
   * 格式化某天的日期
   *
   * @return
   * @throws ParseException
   */
  public static String getDateFormatFromDay(String dayFormat, String day, String resultFormat)
  {
    SimpleDateFormat df = new SimpleDateFormat(dayFormat);
    Date date;

    try {
      date = df.parse(day);
    } catch (Exception e) {
      return null;
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    return new SimpleDateFormat(resultFormat).format(cal.getTime());
  }

  /*
  * 获取某月最后一天的日期
  * */
  /*public static String getLastDayOfMonth(String monthFormat, String month, String dayFormat)
  {
    SimpleDateFormat df = new SimpleDateFormat(monthFormat);
    Date date;

    try {
      date = df.parse(month);
    } catch (Exception e) {
      return null;
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    //获取某月最大天数
    int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
    //设置日历中月份的最大天数
    cal.set(Calendar.DAY_OF_MONTH, lastDay);
    //格式化日期
    SimpleDateFormat sdf = new SimpleDateFormat(dayFormat);
    String lastDayOfMonth = sdf.format(cal.getTime());

    return lastDayOfMonth;
  }*/

  public static Date getBeginOfDay(Date date)
  {
    String beginDay = new SimpleDateFormat(EARER_IN_THE_DAY).format(date);
    try {
      return parserStringToDate(beginDay, LONG_DATE_FORMAT_STR);
    } catch (ParseException e) {
      LOG.error(e);
    }
    return null;
  }

  public static Date getEndOfDay(Date date)
  {
    String endDay = new SimpleDateFormat(LATE_IN_THE_DAY).format(date);
    try {
      return parserStringToDate(endDay, MAX_LONG_DATE_FORMAT_STR);
    } catch (ParseException e) {
      LOG.error(e);
    }
    return null;
  }

  public static String formatDate(Date date, String format)
  {
    DateFormat dateFormat = new SimpleDateFormat(format);
    return dateFormat.format(date);
  }

  public static String formatDate4ShortPattern(Date date)
  {
    return formatDate(date, SHORT_DATE_FORMAT_STR);
  }

  public static Calendar getCalendar(Date date)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar;
  }

  public static long truncateDate(Date beginDate, Date endDate)
  {
    if (endDate != null && beginDate != null) {
      GregorianCalendar end = new GregorianCalendar();
      end.setTime(endDate);

      GregorianCalendar begin = new GregorianCalendar();
      begin.setTime(beginDate);

      return (end.getTimeInMillis() - begin.getTimeInMillis()) / DAY_LONG;
    }
    return 0;
  }

  public static Date addDays(Date current, int days)
  {
    if (null == current) {
      return null;
    }
    return addDays(current, days, "d");
  }

  public static Date addDays(Date current, int time, String unit)
  {
    if (null == current) {
      return null;
    }

    if ("d".equalsIgnoreCase(unit)) {
      return new Date(current.getTime() + Long.valueOf(time) * ONE_DAY_SECONDS * 1000L);
    } else if ("h".equalsIgnoreCase(unit)) {
      return new Date(current.getTime() + Long.valueOf(time) * ONE_HOURS_SECONDS * 1000L);
    } else if ("m".equalsIgnoreCase(unit)) {
      return new Date(current.getTime() + Long.valueOf(time) * ONE_MINUTE_SECONDS * 1000L);
    } else if ("s".equalsIgnoreCase(unit)) {
      return new Date(current.getTime() + Long.valueOf(time) * 1000L);
    }
    return new Date(current.getTime() + Long.valueOf(time) * ONE_DAY_SECONDS * 1000L);
  }

  public static boolean areSameDay(Date dateA, Date dateB)
  {
    Calendar calDateA = Calendar.getInstance();
    calDateA.setTime(dateA);

    Calendar calDateB = Calendar.getInstance();
    calDateB.setTime(dateB);

    return calDateA.get(Calendar.YEAR) == calDateB.get(Calendar.YEAR)
      && calDateA.get(Calendar.MONTH) == calDateB.get(Calendar.MONTH)
      && calDateA.get(Calendar.DAY_OF_MONTH) == calDateB.get(Calendar.DAY_OF_MONTH);
  }

  public static boolean isBetween(Date date, Date start, Date end)
  {
    if (start != null && end != null && start.compareTo(end) > 0) {
      throw new RuntimeException("起止日期配置错误：结束日期小于开始日期!");
    }

    if (start == null && end == null) {
      return true;
    }

    if (end == null) {
      return start.compareTo(date) <= 0;
    }

    if (start == null) {
      return end.compareTo(date) >= 0;
    }

    return start.compareTo(date) <= 0 && end.compareTo(date) >= 0;
  }

  public static long truncateDay(long time)
  {
    return org.apache.commons.lang.time.DateUtils.truncate(new Date(time), Calendar.DATE).getTime();
  }

  public static String getFormatedDay(long time)
  {
    return DateFormatUtils.format(time, SHORT_DATE_FORMAT_STR);
  }

  public static String getFormatedDayWithLongFormat(long time)
  {
    return DateFormatUtils.format(time, LONG_DATE_FORMAT_STR);
  }

  public static String getFormatedTimeWithHmFormat(long time)
  {
    return DateFormatUtils.format(time, HOUR_MIN_FORMAT_STR);
  }

  public static String format(Date date, String pattern)
  {

    return DateFormatUtils.format(date, pattern);
  }

  public static void main(String[] args) throws Exception
  {
    for (int i = 0; i < 100; i++) {
      System.out.println(((Double) (Math.random() * 180)).intValue());

    }

    System.out.print(getMaxDay(new Date()));
        /*System.out.println(getTheMorningTime());
        System.out.println(getTheNextWeekTime(1));
        System.out.println(getTheNextMonthTime(1));
        System.out.println(getTheNextYearTime(1));*/

        /*Date date = parserStringToDate("2014-07-04", SHORT_DATE_FORMAT_STR);
        Date start = parserStringToDate("2014-07-01", SHORT_DATE_FORMAT_STR);
        Date end = parserStringToDate("2014-07-03", SHORT_DATE_FORMAT_STR);
        System.out.println(isBetween(date, start, end));
        */

        /*System.out.println(getSpecailPatten(new Date()));*/

        /*
        Date date1 = parserStringToDate("2014-07-02", SHORT_DATE_FORMAT_STR);
        Date date2 = parserStringToDate("2014-07-01", SHORT_DATE_FORMAT_STR);
        System.out.println(truncateDate(date1, date2));

        System.out.println(formatDate(getFirstOfDay(new Date()),"yyyy-MM-dd HH:mm:ss SSS"));
        System.out.println(formatDate(getEndOfDay(new Date()),"yyyy-MM-dd HH:mm:ss SSS"));
        */
  }

  /**
   * @param day
   * @return
   */
  public static String getWeekFromDay(String day)
  {
    Date date = DateUtils.parse(day, SHORT_DATE_FORMAT_STR);
    Calendar calendar = Calendar.getInstance();
    calendar.setFirstDayOfWeek(Calendar.MONDAY);
    calendar.setTime(date);
    int weekNum = calendar.get(Calendar.WEEK_OF_YEAR);
    int year = calendar.get(Calendar.YEAR);

    //use new year week num at the end of December
    if ((1 == weekNum) && (Calendar.DECEMBER == calendar.get(Calendar.MONTH))) {
      calendar.roll(Calendar.DAY_OF_MONTH, -7);
      if (calendar.get(Calendar.WEEK_OF_YEAR) > weekNum) {
        year++;
      }
    }
    calendar.clear();
    return String.format(WEEKLY_DATE_PATTERN, year, weekNum);
  }

  public static Date getTheMorningTime()
  {
    return getTheMorningTime(1);
  }

  /**
   * 获取第二天凌晨时间
   *
   * @return
   */
  public static Date getTheMorningTime(int period)
  {
    return getTime(Calendar.DATE, period, true);
  }

  private static Date getTime(int calendarUnit, int period, boolean random)
  {
    if (period <= 0) {
      throw new RuntimeException("period 不能小于1");
    }
    Calendar calendar = Calendar.getInstance();
    calendar.add(calendarUnit, period);

    Date date = calendar.getTime();
    SimpleDateFormat formatter = new SimpleDateFormat(DateUtils.SHORT_DATE_FORMAT_STR);
    String dateString = formatter.format(date);

    Date ret = null;
    try {
      ret = formatter.parse(dateString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    if (random) {
      ret = addSecond(ret, ((Double) (Math.random() * 180)).intValue());
    }

    return ret;
  }

  public static Date getTheNextWeekTime(int period)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(getTime(Calendar.WEEK_OF_YEAR, period, true));
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
    return cal.getTime();
  }

  public static Date getTheNextMonthTime(int period)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(getTime(Calendar.MONTH, period, true));
    cal.set(Calendar.DAY_OF_MONTH, 1);
    return cal.getTime();
  }

  public static Date getTheNextYearTime(int period)
  {
    Calendar cal = Calendar.getInstance();
    cal.setTime(getTime(Calendar.YEAR, period, true));
    cal.set(Calendar.DAY_OF_YEAR, 1);
    return cal.getTime();
  }

  public static String getSpecailPatten(Date date)
  {
    String datePattern = formatDate(date, DateUtils.SHORT_DATE_FORMAT_STR);
    return datePattern.replaceAll("-", "\\.");
  }

  public static int convertToMinFromMills(long value)
  {
    return (int) Math.ceil((double) value / (ONE_SECOND_MILLS * ONE_MINUTE_SECONDS));
  }

  /**
   * 时间字符串转换为时间
   *
   * @param dateString 时间字符串 如：2011-11-11
   * @param formatStr  时间字符串类型 如：YMD，YMD_HMS，YMD_HMSZ
   * @return
   */
  public static Date getDate(String dateString, String formatStr)
  {
    SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
    if (dateString == null || dateString.equals("")) {
      return null;
    }
    Date date = null;
    try {
      date = sdf.parse(dateString);
    } catch (ParseException e) {
      LOG.error("getDate", e);
    }
    return date;
  }

  /**
   * 格式化时间
   *
   * @param date
   * @param formatStr 时间字符串类型 如：YMD，YMD_HMS，YMD_HMSZ
   * @return
   */
  public static String getFormatDateStr(Date date, String formatStr)
  {
    SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
    if (date != null) {
      if (formatStr.equals(YMD_HMSZ)) {
        return sdf.format(date) + "000";
      }
      return sdf.format(date);
    }
    return "";
  }

  /**
   * @param dateString
   * @param formatStr
   * @param type       <a>DateUtils.BIG 当日最大时间</a><a>DateUtils.SMALL 当日最小时间</a>
   * @return
   */
  public static Date getDate(String dateString, String formatStr, String type)
  {
    SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
    if (dateString == null || dateString.equals("")) {
      return null;
    }
    Date date = null;
    try {
      //change String to Date
      date = sdf.parse(dateString);
      //change Date parameter
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      format(calendar, type);
      return calendar.getTime();
    } catch (ParseException e) {
      LOG.error("getDate", e);
      return null;
    }
  }

  /**
   * <p>Description:根据类型获取当日时间 </p>
   *
   * @param type DateUtils.BIG or DateUtils.SMALL
   * @return
   * @author crush_lee
   * @date 2014年9月11日
   */
  public static Date getDate(String type)
  {
    Calendar calendar = Calendar.getInstance();
    format(calendar, type);
    return calendar.getTime();
  }

  /**
   * <p>Description:根据类型获取时间 </p>
   *
   * @param date
   * @param type DateUtils.BIG or DateUtils.SMALL
   * @return
   * @author crush_lee
   * @date 2014年9月11日
   */
  public static Date getDate(Date date, String type)
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    format(calendar, type);
    return calendar.getTime();
  }

  private static void format(Calendar calendar, String type)
  {
    if (type.equals(BIG)) {
      calendar.set(Calendar.HOUR, 23);
      calendar.set(Calendar.MINUTE, 59);
      calendar.set(Calendar.SECOND, 59);
    }
    if (type.equals(SMALL)) {
      calendar.set(Calendar.HOUR, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
    }
  }

  public static Date convertString(String value)
  {
    return convertString(value, "yyyy-MM-dd HH:mm:ss");
  }

  public static Date convertString(String value, String format)
  {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    if (value == null) {
      return null;
    }
    try {
      return sdf.parse(value);
    } catch (Exception e) {
      LOG.error("convertString", e);
    }
    return null;
  }

  /*
  * 根据分钟字符串获取天
  * */
  public static String getDayByMinute(String minute) {
    if (StringUtils.isEmpty(minute)) {
      return "";
    }

    return minute.substring(0, 8);
  }

  /*
  * 根据分钟字符串获取小时
  * */
  public static String getHourByMinute(String minute) {
    if (StringUtils.isEmpty(minute)) {
      return "";
    }

    return minute.substring(0, 10);
  }

  /*
  * 根据分钟字符串仅获取小时
  * */
  public static String getOnlyHourByMinute(String minute) {
    if (StringUtils.isEmpty(minute)) {
      return "";
    }

    return minute.substring(8, 10);
  }

  /*
  * 根据分钟字符串仅获取分钟
  * */
  public static String getOnlyMinuteByMinute(String minute) {
    if (StringUtils.isEmpty(minute)) {
      return "";
    }

    return minute.substring(10, 12);
  }

  /*
  * 根据小时字符串获取月
  * */
  public static String getMonthByHour(String hour) {
    if (StringUtils.isEmpty(hour)) {
      return "";
    }

    return hour.substring(0, 6);
  }

  /*
  * 根据小时字符串获取天
  * */
  public static String getDayByHour(String hour) {
    if (StringUtils.isEmpty(hour)) {
      return "";
    }

    return hour.substring(0, 8);
  }

  /*
  * 根据小时字符串仅获取天
  * */
  public static String getOnlyDayByHour(String hour) {
    if (StringUtils.isEmpty(hour)) {
      return "";
    }

    return hour.substring(6, 8);
  }

  /*
  * 根据小时字符串仅获取小时
  * */
  public static String getOnlyHourByHour(String hour) {
    if (StringUtils.isEmpty(hour)) {
      return "";
    }

    return hour.substring(8, 10);
  }

  /*
  * 根据天字符串仅获取天
  * */
  public static String getOnlyDayByDay(String day) {
    if (StringUtils.isEmpty(day)) {
      return "";
    }

    return day.substring(6, 8);
  }
}
