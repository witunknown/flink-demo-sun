package sun.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.flink.source.SourceUtils;
import sun.model.UserInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-24 20:49
 * Desc:
 */
public class KeyedStatueDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(20);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.visitPathSource(100, 2, 1000));
        //根据性别分key
        source.keyBy(t -> t.getSex()).flatMap(new FlatMapOperatorState()).print();
        env.execute("KeyedStatueDemo");
    }
}
