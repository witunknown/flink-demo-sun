package sun.flink.waterMark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import sun.model.UserInfo;
import sun.utils.DateUtils;

import javax.annotation.Nullable;

/**
 * Created byX on 2021-02-03 17:24
 * Desc:
 */
public class MyPeriodWaterMark implements AssignerWithPeriodicWatermarks<UserInfo> {

    //second default 10s
    private long maxDelay = 10;

    private long maxTimeStamp;

    public MyPeriodWaterMark() {
    }

    public MyPeriodWaterMark(long maxDelay) {
        this.maxDelay = maxDelay;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
//        System.out.println("system time is:" + DateUtils.getDate(System.currentTimeMillis()));
        return new Watermark(maxTimeStamp - maxDelay * 1000);
    }

    @Override
    public long extractTimestamp(UserInfo element, long previousElementTimestamp) {
        String date = element.getVisitTime();
        long timestamp = DateUtils.getTimestamp(date);
        if (timestamp > maxTimeStamp) {
            maxTimeStamp = timestamp;
        }
        return timestamp;
    }
}
