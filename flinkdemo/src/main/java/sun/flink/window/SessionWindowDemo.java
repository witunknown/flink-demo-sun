package sun.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.flink.sink.ClickHouseSink;
import sun.flink.source.SourceUtils;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;
import sun.model.UserVisitInfo;
import sun.utils.DateUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-08 16:07
 * Desc: session 窗口，基于相同keyed id 的间隔时间
 */
public class SessionWindowDemo {
    public static <visitPath> void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.visitPathSource(10000, 20, 1000));
        source.assignTimestampsAndWatermarks(new MyPeriodWaterMark()).map(t -> {
            return new Tuple2<String, String>(t.getId(), t.getVisitPage());
        }).returns(Types.TUPLE(Types.STRING, Types.STRING)).keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).process(new ProcessWindowFunction<Tuple2<String, String>, UserVisitInfo, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, String>> elements, Collector<UserVisitInfo> out) throws Exception {
                StringBuilder path = new StringBuilder("startFlag");
                String uid = elements.iterator().next().f0;
                elements.forEach(element -> {
                    path.append(">").append(element.f1);
                });
                String start = DateUtils.getDate(context.window().getStart());
                String end = DateUtils.getDate(context.window().getEnd());
                out.collect(new UserVisitInfo(uid, path.toString(), start, end));
//                out.collect(new Tuple2<String, String>(start + "--" + end + "::" + uid.substring(0, 6), path.toString()));
            }
        }).setParallelism(3).addSink(new ClickHouseSink());

        env.execute("sesionWindow demo");
    }

}
