package com.learnFlink.multiStreams;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class unionStreams {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<JSONObject> stream1 = env.fromElements(1, 3, 5, 7).map((MapFunction<Integer, JSONObject>) integer -> new JSONObject().fluentPut("val", integer).fluentPut("src", "s1"));


        SingleOutputStreamOperator<JSONObject> stream2 = env.fromElements(100L, 200L, 600L, 700L).map((MapFunction<Long, JSONObject>) aLong -> new JSONObject().fluentPut("val", aLong).fluentPut("src", "s2"));

        stream1.union(stream2).print();



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
