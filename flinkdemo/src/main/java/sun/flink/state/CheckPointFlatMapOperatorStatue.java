package sun.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import sun.model.UserInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Created byX on 2021-02-25 00:15
 * Desc:
 */
public class CheckPointFlatMapOperatorStatue extends RichFlatMapFunction<UserInfo, Tuple2<String, String>> implements CheckpointedFunction {

    private transient ListState<String> maleVisitPages;
    private transient ListState<String> femaleVisitPages;

    private List<String> maleListBuffer = new ArrayList<>();
    private List<String> femaleListBuffer = new ArrayList<>();

    @Override
    public void flatMap(UserInfo value, Collector<Tuple2<String, String>> out) throws Exception {
        if (value.getSex().equals("0")) {
            String page = value.getVisitPage();
            maleListBuffer.add(page);
            if (maleListBuffer.size() > 5) {
                out.collect(new Tuple2<>("0", maleListBuffer.stream().reduce((x, y) -> x + ">" + y).get()));
                maleListBuffer.clear();
            }
        } else {
            String page = value.getVisitPage();
            femaleListBuffer.add(page);
            if (femaleListBuffer.size() > 5) {
                out.collect(new Tuple2<>("1", femaleListBuffer.stream().reduce((x, y) -> x + "<" + y).get()));
                femaleListBuffer.clear();
            }
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //先清理，再全量加；
        maleVisitPages.clear();
        for (String item : maleListBuffer) {
            maleVisitPages.add(item);
        }

        femaleVisitPages.clear();
        for (String item : femaleListBuffer) {
            femaleVisitPages.add(item);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //1.定义描述
        ListStateDescriptor<String> maleDescriptor = new ListStateDescriptor<String>("maleVisitPage", String.class);
        //获取句柄
        maleVisitPages = context.getOperatorStateStore().getListState(maleDescriptor);
        //若处于数据恢复阶段
        if (context.isRestored()) {
            for (String item : maleVisitPages.get()) {
                maleListBuffer.add(item);
            }
        }

        //1.定义描述
        ListStateDescriptor<String> femaleDescriptor = new ListStateDescriptor<String>("femaleVisitPage", String.class);
        //获取句柄
        femaleVisitPages = context.getOperatorStateStore().getListState(femaleDescriptor);
        //若处于数据恢复阶段
        if (context.isRestored()) {
            for (String item : femaleVisitPages.get()) {
                femaleListBuffer.add(item);
            }
        }
    }
}
