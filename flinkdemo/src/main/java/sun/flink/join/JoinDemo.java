package sun.flink.join;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import sun.flink.source.SourceUtils;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-24 00:02
 * Desc:基于间隔的join
 */
public class JoinDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<UserInfo> sourceA = env.addSource(SourceUtils.visitPathSource(2, 2, 1000));
        DataStreamSource<UserInfo> sourceB = env.addSource(SourceUtils.visitPathSource(2, 2, 1000));
        SingleOutputStreamOperator<Tuple2<String, String>> joinA = sourceA.assignTimestampsAndWatermarks(new MyPeriodWaterMark(5)).map(t -> {
            return new Tuple2<String, String>(t.getId(), t.getVisitPage());
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        SingleOutputStreamOperator<Tuple2<String, String>> joinB = sourceB.assignTimestampsAndWatermarks(new MyPeriodWaterMark(5)).map(t -> {
            return new Tuple2<String, String>(t.getId(), t.getVisitPage());
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));
        joinA.keyBy(0).intervalJoin(joinB.keyBy(0)).between(Time.seconds(0), Time.seconds(5)).process(new UDFJoinFunction()).print();

        env.execute("interval join");
    }
}
