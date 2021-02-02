package sun.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.UUID;

/**
 * Created byX on 2021-02-03 00:39
 * Desc:
 */
public class RandomUtils {


    public static String getRandomNameDefault() throws Exception {
        int length = 10;
        return getRandomName(length);
    }

    public static String getRandomName(int length) throws Exception {
        if (length <= 0) {
            throw new IllegalArgumentException("length参数有误");
        }
        String s = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        char[] c = s.toCharArray();
        Random random = new Random();
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < length; i++) {
            name.append(c[random.nextInt(c.length)]);
        }
        return name.toString();
    }


    public static int getRandomAbsIntDefault() {
        int maxInt = 100;
        return getRandomAbsInt(maxInt);
    }

    public static int getRandomAbsInt(int maxInt) {
        if (maxInt <= 0) {
            maxInt = Math.abs(maxInt);
        }
        Random random = new Random();
        return Math.abs(random.nextInt() % maxInt);
    }

    /**
     * @return 1 or 0; 1:male 0:female
     */
    public static String getRandomSex() {
        int maxInt = 2;
        return String.valueOf(getRandomAbsInt(maxInt));
    }

    public static int getRandomScore(int maxSource) {
        return getRandomAbsInt(maxSource);
    }

    public static String getRandomId() {
        return UUID.randomUUID().toString();
    }

    /**
     * @param startTime yyyy-MM-dd hh:mm:ss
     * @param period    second
     */
    public static String getRandomTime(String startTime, int period) {
        String resRandomTime = "";

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        try {
            Date start = format.parse(startTime);
            boolean pos = true;
            if (period < 0) {
                pos = false;
            }
            //trans to ms
            int randomPeriodMs = getRandomAbsInt(period * 1000);
            long randomTimeStamp = pos == true ? start.getTime() + randomPeriodMs : start.getTime() - randomPeriodMs;
            resRandomTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(randomTimeStamp);
        } catch (ParseException e) {
            throw new IllegalStateException("时间转换异常");
        }
        return resRandomTime;
    }


    public static String getRandomTimeBaseCurrentTime(int period) {
        String resRandomTime = "";

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        long currentTimestamp = System.currentTimeMillis();
        boolean pos = true;
        if (period < 0) {
            pos = false;
        }
        //trans to ms
        int randomPeriodMs = getRandomAbsInt(period * 1000);
        long randomTimeStamp = pos == true ? currentTimestamp + randomPeriodMs : currentTimestamp - randomPeriodMs;
        resRandomTime = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(randomTimeStamp);
        return resRandomTime;
    }

}
