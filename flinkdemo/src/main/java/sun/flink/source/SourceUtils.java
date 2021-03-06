package sun.flink.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.utils.RandomUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-07 16:03
 * Desc:
 */
public class SourceUtils {
    private static Logger log = LoggerFactory.getLogger(MySource.class.getClass().getName());

    public static MySource IncreaseTimeSource(int eventNum) {
        if (eventNum <= 0) {
            throw new IllegalArgumentException("参数异常");
        }

        AtomicInteger num = new AtomicInteger(eventNum);
        MySource mySource = new MySource(num);
        mySource.setTimeRange(1);
        return mySource;
    }

    public static MySource visitPathSource(int eventNum, int uidPoolSize, int freq) {
        if (eventNum <= 0 || uidPoolSize <= 0 || freq <= 0) {
            throw new IllegalArgumentException("参数异常");
        }
        AtomicInteger num = new AtomicInteger(eventNum);
        MySource mySource = new MySource(num, uidPoolSize, freq);
        mySource.setTimeRange(1);
        return mySource;
    }

    /**
     *
     * @param eventNum
     * @param uidPoolSize
     * @param freq   数据发送频率 ms
     * @param rangeTime   数据发送区间，以当前时间
     * @return
     */
    public static MySource rangeTimeSource(int eventNum, int uidPoolSize, int freq, int rangeTime) {
        if (eventNum <= 0 || uidPoolSize <= 0 || freq <= 0 || rangeTime <= 0) {
            throw new IllegalArgumentException("参数异常");
        }
        AtomicInteger num = new AtomicInteger(eventNum);
        MySource mySource = new MySource(num, uidPoolSize, freq);
        mySource.setTimeRange(rangeTime);
        return mySource;
    }
}
