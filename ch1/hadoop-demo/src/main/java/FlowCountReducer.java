import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        // 遍历所有的bean,将其中的上行流量,下行流量分别累加
        for (FlowBean bean : values) {
            sumUpFlow += bean.getUpFlow();
            sumDownFlow += bean.getDownFlow();
        }
        FlowBean resultBean = new FlowBean(sumUpFlow, sumDownFlow);
        context.write(key, resultBean);
    }
}
