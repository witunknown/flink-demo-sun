package sun.flink.delay;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sun.flink.source.SourceUtils;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;
import sun.utils.DateUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-19 13:02
 * Desc:
 */
public class SideOutPutDemo {

    private static final OutputTag<UserInfo> late = new OutputTag<UserInfo>("lateUser"){};


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(10);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.rangeTimeSource(100, 1, 1000, 10));
        SingleOutputStreamOperator<String> stream = source.assignTimestampsAndWatermarks(new MyPeriodWaterMark(1)).keyBy(t -> {
            return t.getId();
        }).window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(1)).sideOutputLateData(late).process(new ProcessWindowFunction<UserInfo, String, String, TimeWindow>() {

            @Override
            public void process(String s, Context context, Iterable<UserInfo> elements, Collector<String> out) throws Exception {
                String start = DateUtils.getDate(context.window().getStart());
                String end = DateUtils.getDate(context.window().getEnd());
                String uid = elements.iterator().next().getId();
                out.collect(uid + "::" + start + "::" + end+":: current:"+DateUtils.getDate(System.currentTimeMillis()));
            }
        });
//        source.print();
        stream.print("正常数据:");
        stream.getSideOutput(late).print("迟到的数据:");
        env.execute("my source");
    }
}
