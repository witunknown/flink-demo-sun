package sun.flink.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sun.flink.source.SourceUtils;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-24 01:10
 * Desc:基于窗口的join
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<UserInfo> sourceA = env.addSource(SourceUtils.visitPathSource(100, 2, 1000));
        DataStreamSource<UserInfo> sourceB = env.addSource(SourceUtils.visitPathSource(100, 2, 1000));
        SingleOutputStreamOperator<Tuple2<String, String>> joinA = sourceA.assignTimestampsAndWatermarks(new MyPeriodWaterMark(5)).map(t -> {
            return new Tuple2<String, String>(t.getId(), t.getVisitPage());
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        SingleOutputStreamOperator<Tuple2<String, String>> joinB = sourceB.assignTimestampsAndWatermarks(new MyPeriodWaterMark(5)).map(t -> {
            return new Tuple2<String, String>(t.getId(), t.getVisitPage());
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        joinA.join(joinB).where(A -> A.f0).equalTo(B -> B.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(2)).apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> join(Tuple2<String, String> first, Tuple2<String, String> second) throws Exception {
                return new Tuple2<String, String>(first.f0.substring(0, 2), first.f1 + ">" + second.f1);
            }
        }).print("执行结果");
        env.execute("window join");
    }
}
