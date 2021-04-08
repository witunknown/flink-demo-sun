package sun.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import sun.flink.source.SourceUtils;
import sun.flink.waterMark.MyPeriodWaterMark;
import sun.model.UserInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-08 17:47
 * Desc:基于event 事件
 */
public class TumblingWindowBaseEventDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.visitPathSource(100, 5, 2000));
        source.assignTimestampsAndWatermarks(new MyPeriodWaterMark()).map(t -> {
            return new Tuple2<String, UserInfo>(t.getId(), t);
        }).returns(Types.TUPLE(Types.STRING, Types.POJO(UserInfo.class))).keyBy(0).countWindow(10).apply(new WindowFunction<Tuple2<String, UserInfo>, String, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple2<String, UserInfo>> input, Collector<String> out) throws Exception {
                String uid = input.iterator().next().f0;
                AtomicInteger totalScore = new AtomicInteger();
                AtomicInteger num = new AtomicInteger();
                input.forEach(t -> {
                    totalScore.getAndAdd(t.f1.getScore());
                    num.getAndIncrement();
                });
                int average = totalScore.get() / num.get();
                out.collect("uid:"+uid+";average:"+average+": num="+num.get());
            }
        }).print();
        env.execute("tumblingWindow demo");
    }
}
