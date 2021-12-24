package com.example.bean;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 1. 继承Writable接口
@Data   // 2. 提供getter/setter方法
@NoArgsConstructor  // 3. 提供无参构造器
public class FlowBean implements Writable {

    private long upFlow;    // 上行流量
    private long downFlow;  // 下行流量
    private long sumFlow;   // 总流量

    // 4. 实现序列化和反序列化方法，注意顺序一定要保持一致，该顺序是指三个属性的序列化顺序
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    // 反序列化，注意与上面序列化的顺序必须保持一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    // 5. 重写 toString 方法
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void setSumFlow(){
        this.sumFlow = this.upFlow + this.downFlow;
    }
}
