package sun.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import sun.model.UserInfo;

/**
 * Created byX on 2021-02-24 20:04
 * Desc:
 */
public class FlatMapOperatorState extends RichFlatMapFunction<UserInfo, Tuple2<String, String>> {

    private transient ValueState<Tuple2<String, String>> sumStatue;

    @Override
    public void flatMap(UserInfo value, Collector<Tuple2<String, String>> out) throws Exception {
        //超过60分的，每5个人打印用户id和访问页
        if (value.getScore() >= 60) {
            Tuple2<String, String> current = sumStatue.value();
            if (null == current) {
                current = new Tuple2<String, String>();
                sumStatue.update(current);
            } else if (current.f0.split(";").length > 5) {
                out.collect(new Tuple2<>(current.f0, current.f1));
                sumStatue.clear();
            } else {
                sumStatue.update(new Tuple2<>(current.f0 + ";" + value.getId().substring(0, 2), current.f1 + ">" + value.getVisitPage()));
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<String, String>> descriptor = new ValueStateDescriptor<Tuple2<String, String>>("sum", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        //获取状态句柄
        sumStatue = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
