package sun.flink.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sun.flink.source.MySource;
import sun.flink.source.SourceUtils;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.flink.waterMark.MyPunctuateWaterMarks;
import sun.model.UserInfo;
import sun.utils.DateUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-07 10:01
 * Desc:
 */
public class PunctuateWaterMarkDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.IncreaseTimeSource(100));
        source.assignTimestampsAndWatermarks(new MyPunctuateWaterMarks()).addSink(new SinkFunction<UserInfo>() {
            @Override
            public void invoke(UserInfo value, Context context) throws Exception {
//                String waterMark = DateUtils.getDate(context.currentWatermark());
                System.out.println("id:" + value.getId() + "  sex:" + value.getSex() + ";  waterMark is :" + (context.currentWatermark() > 0 ? DateUtils.getDate(context.currentWatermark()) : context.currentWatermark()) + " event time:" + value.getVisitTime());
            }
        }).setParallelism(1);

        env.execute("waterMark");
    }
}
