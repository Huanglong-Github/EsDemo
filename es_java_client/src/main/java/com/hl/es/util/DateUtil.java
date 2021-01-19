package com.hl.es.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: huanglong60
 * @date: 2021/1/15 10:15
 * @description:
 */
public class DateUtil {
    public static final String DEFAULT_DATE_FORMATE = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMATE_1 = "yyyy-MM-dd";
    public static final String yyyyMMdd_HHmm = "yyyy-MM-dd HH:mm";
    public static final int TIME_DAY_MILLISECOND = 86400000;
    private static final int RATIO = 1000;
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATE_FORMAT3 = "HH:mm";
    public static final String DATE_FORMAT_4 = "yyyyMMddHH";
    public static final String DATE_FORMAT_5 = "M月d日";
    public static final String DATE_FORMAT_6 = "yyyyMMdd";
    public static final String DATE_FORMAT_7 = "yyyy年MM月dd日";
    public static final String DATE_FORMAT_8 = "HH:mm:ss";
    public static final String yyyyMMdd_HHmmss = "yyyy-MM-dd HH:mm:ss";
    public static final String yyyyMMdd_HHmmss_SSS = "yyyy-MM-dd HH:mm:ss SSS";

    /**
     * 功能描述：将时间数据转换为指定格式的字符串并返回
     * @param currDate
     * @param format
     * @return
     */
    public static String getFormatDate(Date currDate, String format) {
        if (currDate == null) {
            return "";
        } else {
            SimpleDateFormat dtFormatdB = null;
            try {
                dtFormatdB = new SimpleDateFormat(format);
                return dtFormatdB.format(currDate);
            } catch (Exception var6) {
                dtFormatdB = new SimpleDateFormat("yyyy-MM-dd");

                try {
                    return dtFormatdB.format(currDate);
                } catch (Exception var5) {
                    return null;
                }
            }
        }
    }
}
