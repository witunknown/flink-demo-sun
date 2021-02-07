package sun.flink.waterMark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import sun.model.UserInfo;
import sun.utils.DateUtils;

import javax.annotation.Nullable;

/**
 * Created byX on 2021-02-07 15:14
 * Desc:
 */
public class MyPunctuateWaterMarks implements AssignerWithPunctuatedWatermarks<UserInfo> {
    long bound = 5 * 1000;

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(UserInfo lastElement, long extractedTimestamp) {
        if (lastElement.getSex().equals("0")) {
            System.out.println("id:"+lastElement.getId()+"更新水印");
            return new Watermark(extractedTimestamp - bound);
        }
        return null;
    }

    @Override
    public long extractTimestamp(UserInfo element, long previousElementTimestamp) {
        return DateUtils.getTimestamp(element.getVisitTime());
    }
}
