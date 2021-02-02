package sun.flink.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.flink.source.MySource;
import sun.model.UserInfo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-03 01:27
 * Desc:
 */
public class MySorceWindow {

    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        AtomicInteger num=new AtomicInteger(1000);
        DataStreamSource<UserInfo> source = env.addSource(new MySource(num));
        source.print();
        env.execute("my source");
    }
}
