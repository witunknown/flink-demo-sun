package sun.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created byX on 2021-02-03 17:40
 * Desc:
 */
public class DateUtils {
    private static final String FULL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static String getDate(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("参数异常");
        }

        Date date = new Date(timestamp);
        SimpleDateFormat format = new SimpleDateFormat(FULL_DATE_FORMAT);
        return format.format(date);
    }


    public static long getTimestamp(String dateStr) {

        SimpleDateFormat sdf = new SimpleDateFormat(FULL_DATE_FORMAT);
        Date date = null;
        try {
            date = sdf.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long timestamp = date.getTime();
        return timestamp;
    }


    public static void main(String[] args) {
        System.out.println(DateUtils.getDate(System.currentTimeMillis()));
    }
}
