package sun.flink.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.flink.source.MySource;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;
import sun.utils.DateUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-03 17:03
 * Desc:滚动窗口，基于时间
 */
public class TumblingWindowDemo {

    private static Logger log = LoggerFactory.getLogger(FlinkSessionWindowDemo.class.getClass().getName());

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);
        DataStreamSource<UserInfo> source = env.addSource(new MySource(num));
        source.assignTimestampsAndWatermarks(new MyPeriodWaterMark(10))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<UserInfo, String, TimeWindow>() {

                    AtomicInteger total = new AtomicInteger(0);

                    @Override
                    public void process(Context context, Iterable<UserInfo> elements, Collector<String> out) throws Exception {
                        String startStr = DateUtils.getDate(context.window().getStart());
                        String endStr = DateUtils.getDate(context.window().getEnd());
                        elements.forEach(t -> {
                            total.getAndAdd(1);
                        });
                        out.collect("system :"+DateUtils.getDate(System.currentTimeMillis())+"start:" + startStr + ";end:" + endStr + ";total:" + total.get());
                    }
                }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute("tumblingWindow");

    }
}
