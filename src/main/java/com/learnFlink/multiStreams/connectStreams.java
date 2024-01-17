package com.learnFlink.multiStreams;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class connectStreams {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 3, 5, 7);


        DataStreamSource<Long> stream2 = env.fromElements(100L, 200L, 600L, 700L);

        stream1.connect(stream2).process(new CoProcessFunction<Integer, Long, JSONObject>() {

            @Override
            public void processElement1(Integer integer, CoProcessFunction<Integer, Long, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                collector.collect(new JSONObject().fluentPut("val", integer).fluentPut("src", "s1"));
            }

            @Override
            public void processElement2(Long aLong, CoProcessFunction<Integer, Long, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                collector.collect(new JSONObject().fluentPut("val", aLong).fluentPut("src", "s2"));
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
