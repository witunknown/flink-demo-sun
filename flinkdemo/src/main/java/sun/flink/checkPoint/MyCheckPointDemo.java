package sun.flink.checkPoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.flink.source.SourceUtils;
import sun.model.UserInfo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-20 00:59
 * Desc:检查点配制
 */
public class MyCheckPointDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num = new AtomicInteger(100);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStateBackend(new FsStateBackend("file:/checkPoint/demo"));
        //开启checkPoint,生成checkPoint的时间间隔
        env.enableCheckpointing(1000L);
        //checkPonit 模式精确一次和至少一次；精确一次代价较高，低延迟时设置至少一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //状态后端生成checkPoint的超时时间，超时则删除
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //检查点生成最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //生成检查点的最大并行度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //flink任务失败或取消后，checkPoint是删除还是保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //当存在较新的savePoint时，是否从检查点恢复；
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        //容忍checkPoint最大失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 在5分钟内，只能重启5次，每次失败后最少需要等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));

        DataStreamSource<UserInfo> source = env.addSource(SourceUtils.visitPathSource(1000, 5, 2000));
        source.print();
        env.execute("checkPoint demo");

    }


}
