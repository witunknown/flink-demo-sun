package sun.flink.join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.flink.window.FlinkSessionWindowDemo;
import sun.utils.DateUtils;

/**
 * Created byX on 2021-02-24 00:30
 * Desc:
 */
public class UDFJoinFunction extends ProcessJoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {

    private static Logger log = LoggerFactory.getLogger(FlinkSessionWindowDemo.class.getClass().getName());

    @Override
    public void processElement(Tuple2<String, String> left, Tuple2<String, String> right, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        String path = left.f1 + ">" + right.f1;
        log.info("左边时间：{}，右边时间{}", DateUtils.getDate(ctx.getLeftTimestamp()), DateUtils.getDate(ctx.getRightTimestamp()));
        out.collect(new Tuple2<>(left.f0.substring(0,2), path+"=="+DateUtils.getDate(ctx.getLeftTimestamp())+"=="+DateUtils.getDate(ctx.getRightTimestamp())));
    }
}
