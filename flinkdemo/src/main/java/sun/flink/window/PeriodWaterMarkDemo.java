package sun.flink.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sun.flink.source.MySource;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;
import sun.utils.DateUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-07 10:01
 * Desc:
 */
public class PeriodWaterMarkDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setAutoWatermarkInterval(10000);
        DataStreamSource<UserInfo> source = env.addSource(new MySource(num));
        source.assignTimestampsAndWatermarks(new MyPeriodWaterMark(10)).addSink(new SinkFunction<UserInfo>() {
            @Override
            public void invoke(UserInfo value, Context context) throws Exception {
//                String waterMark = DateUtils.getDate(context.currentWatermark());
                System.out.println(value.getVisitTime() + ";  waterMark is :" + (context.currentWatermark() > 0 ? DateUtils.getDate(context.currentWatermark()) : context.currentWatermark()));
            }
        });

        env.execute("waterMark");
    }
}
