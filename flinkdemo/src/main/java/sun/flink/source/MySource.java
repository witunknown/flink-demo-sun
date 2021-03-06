package sun.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.flink.window.FlinkSessionWindowDemo;
import sun.model.UserInfo;
import sun.utils.RandomUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created byX on 2021-02-03 00:19
 * Desc:
 */
public class MySource extends RichSourceFunction<UserInfo> {

    private static Logger log = LoggerFactory.getLogger(MySource.class.getClass().getName());

    /**
     * 发送消息量
     */
    private AtomicInteger num;

    private int timeRange = 30;

    private int uidPoolSize = 0;
    //ms
    private int eventFreq = 500;

    public AtomicInteger getNum() {
        return num;
    }

    public void setNum(AtomicInteger num) {
        this.num = num;
    }

    public int getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(int timeRange) {
        this.timeRange = timeRange;
    }

    private volatile boolean isRunning = true;

    public MySource(AtomicInteger num) {
        this.num = num;

    }


    public MySource(AtomicInteger num, int uidPoolSize) {
        this.num = num;
        this.uidPoolSize = uidPoolSize;
    }

    public MySource(AtomicInteger num, int uidPoolSize, int eventFreq) {
        this.num = num;
        this.uidPoolSize = uidPoolSize;
        this.eventFreq = eventFreq;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<UserInfo> ctx) throws Exception {
        while (isRunning && num.get() > 0) {
            String name = RandomUtils.getRandomNameDefault();

            String id = this.uidPoolSize == 0 ? RandomUtils.getRandomId() : RandomUtils.getUidFormPool(uidPoolSize);
            String sex = RandomUtils.getRandomSex();
            int source = RandomUtils.getRandomScore(100);
            String visitTime = RandomUtils.getRandomTimeBaseCurrentTime(timeRange);
            String visitPage = RandomUtils.getVisitPath();
            UserInfo user = new UserInfo(id, name, sex, visitTime, source, visitPage);
            TimeUnit.MILLISECONDS.sleep(eventFreq);
            ctx.collect(user);
            num.getAndDecrement();
        }
        log.info("生成数据停止");

    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
