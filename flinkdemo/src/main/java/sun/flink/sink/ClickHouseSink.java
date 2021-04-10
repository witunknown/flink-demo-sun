package sun.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import sun.model.UserVisitInfo;
import sun.utils.DataBaseUtils;

import java.sql.*;

/**
 * @author Sun
 * @version 1.0.0
 * @ClassName ClickHouseSink.java
 * @Description TODO
 * @createTime 2021年04月07日 22:12:00
 */
public class ClickHouseSink extends RichSinkFunction<UserVisitInfo> {

    private static Statement statement;


    @Override
    public void open(Configuration parameters) throws Exception {
        statement = DataBaseUtils.getConnection().createStatement();
    }

    @Override
    public void invoke(UserVisitInfo userVisitInfo, Context context) throws Exception {
        String sql = String.format("insert into user_visit_page values('%s','%s','%s','%s')", userVisitInfo.getUid(), userVisitInfo.getPath(), userVisitInfo.getStartDateTime(), userVisitInfo.getEndDateTime());
        System.out.println(statement.toString());
        statement.execute(sql);
    }

    @Override
    public void close() throws Exception {
        statement.close();
        DataBaseUtils.getConnection().close();
    }
}
