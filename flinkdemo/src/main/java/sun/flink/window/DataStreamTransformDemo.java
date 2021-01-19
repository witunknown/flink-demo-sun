package sun.flink.window;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * Created byX on 2021-01-19 23:53
 * Desc:
 */
public class DataStreamTransformDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "7071");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.addSource(new MySource()).keyBy(0).timeWindow(Time.seconds(5)).fold("start", new FoldFunction<Tuple2<String, Long>, String>() {
            @Override
            public String fold(String accumulator, Tuple2<String, Long> value) throws Exception {
                return accumulator+"-"+value.f1;
            }
        }).print();
        env.execute("hello world");

    }


    private static class MySource implements SourceFunction<Tuple2<String, Long>> {
        private long count = 0;
        boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            while (isRunning && count < 1000) {
                ctx.collect(new Tuple2<String, Long>("a", count));
                TimeUnit.SECONDS.sleep(1);
                count++;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
