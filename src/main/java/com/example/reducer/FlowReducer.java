package com.example.reducer;

import com.example.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// <Text, FlowBean, Text, FlowBean> 第一个 Text 是输入的 key， 第一个 FlowBean 是输入的 value； 第二个 Text 是输出的 key
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalUp = 0L;
        long totalDown = 0L;

        // 1. 遍历values，将其中的上行流量，下行流量分别累加
        for (FlowBean flowBean : values) {
            totalUp += flowBean.getUpFlow();
            totalDown += flowBean.getDownFlow();
        }

        // 2. 封装 outKV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        // 3. 写出 outK outV
        context.write(key, outV);
    }
}
