package com.example.mapper;

import com.example.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行数据，转成字符串
        String line = value.toString();

        // 2. 分割数据
        String[] split = line.split("\\s"); // 以正则 \s 匹配空白

        // 3. 筛选出我们需要的数据：手机号，上行流量，下行流量
        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];

        // 4. 封装 outK  outV
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();

        // 5. 写出 outK  outV
        context.write(outK, outV);
    }
}
