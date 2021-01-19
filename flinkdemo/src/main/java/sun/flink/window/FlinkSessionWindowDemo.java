package sun.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created byX on 2021-01-13 23:58
 * Desc:
 */
public class FlinkSessionWindowDemo {

    private static Logger log = LoggerFactory.getLogger(FlinkSessionWindowDemo.class.getClass().getName());

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port","7071");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //cmd： nc -l -p 9997
        //input format : yyyy-MM-dd hh:mm:ss,A,1
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9998);
        SingleOutputStreamOperator<String> waterMarkDataStream = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(60)) {

            @Override
            public long extractTimestamp(String s) {
                String[] fields = s.split(",");
                String dateStr = fields[0];
                try {
                    Date date = sdf.parse(dateStr);
                    long timestamp = date.getTime();
                    return timestamp;
                } catch (ParseException e) {
                    throw new RuntimeException("时间转换异常");
                }
            }
        });

        waterMarkDataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] item = value.split(",");
                log.info("item:{},{}",item[1],item[2]);
                return new Tuple2(item[1], Integer.valueOf(item[2]));
            }
        }).keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
            }
        }).print();

        env.execute("hello world");
    }
}
