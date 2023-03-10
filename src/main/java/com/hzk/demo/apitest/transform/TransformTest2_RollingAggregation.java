package com.hzk.demo.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/7 15:09
 */

import com.hzk.demo.apitest.beans.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 聚合算子
 */
//vmoption:
//-Dlog.file=D:\project\flink-test\src\main\resources\test\log\test.log
//        -Dlog4j.configuration=file:\D:\project\flink-test\src\main\resources\log4j.properties
//        -Dlog4j.configurationFile=file:\D:\project\flink-test\src\main\resources\log4j.properties
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\project\\flink-test\\src\\main\\resources\\test\\sensor.txt");

        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());

        DataStream<Long> dataStream1 = env.fromElements(1L, 34L, 4L, 657L, 23L);
        KeyedStream<Long, Integer> keyedStream2 = dataStream1.keyBy(new KeySelector<Long, Integer>() {
            @Override
            public Integer getKey(Long value) throws Exception {
                return value.intValue() % 2;
            }
        });


        // 滚动聚合，取当前最大的温度值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print("result");

        keyedStream1.print("key1");
        keyedStream2.sum(0).print("key2");
        env.execute();
    }
}
