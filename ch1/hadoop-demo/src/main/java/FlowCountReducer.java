import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        // 遍历所有的bean,将其中的上行流量,下行流量分别累加
        for (FlowBean bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }
        FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
        context.write(key, resultBean);
    }
}
