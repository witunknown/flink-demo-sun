package sun.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import sun.model.UserInfo;
import sun.utils.RandomUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-03 00:19
 * Desc:
 */
public class MySource extends RichSourceFunction<UserInfo> {

    /**
     * 发送消息量
     */
    private AtomicInteger num;


    private volatile boolean isRunning = true;

    public MySource(AtomicInteger num) {
        this.num = num;

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        //
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<UserInfo> ctx) throws Exception {
        while (isRunning&&num.get()>=0) {
            String name = RandomUtils.getRandomNameDefault();
            String id = RandomUtils.getRandomId();
            String sex = RandomUtils.getRandomSex();
            int source = RandomUtils.getRandomScore(100);
            String visitTime = RandomUtils.getRandomTimeBaseCurrentTime(30);
            UserInfo user = new UserInfo(id, name, sex, visitTime, source);
            TimeUnit.MILLISECONDS.sleep(500);
            ctx.collect(user);
            num.getAndDecrement();
        }
    }

    @Override
    public void cancel() {
                isRunning=false;
    }


    public static void main(String[] args) throws Exception {

    }
}
