package sun.flink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.flink.source.SourceUtils;
import sun.model.UserInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-25 00:14
 * Desc:
 */
public class CheckPointStatusDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(20);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStateBackend(new RocksDBStateBackend("file:/rocksDB/checkPoint/demo"));
        env.getConfig().setAutoWatermarkInterval(1000);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.visitPathSource(100, 2, 1000));
        source.flatMap(new CheckPointFlatMapOperatorStatue()).setParallelism(3).print("visit path:");
        env.execute("operator status");
    }
}
