package com.learnFlink.windowProcessing;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class keyedEventTimeWindowWithProcessWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new selfDefineLogPojoSource())
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<LogPOJO>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<LogPOJO>) (logPOJO, l) -> System.currentTimeMillis())
                )
                .keyBy((KeySelector<LogPOJO, Object>) logPOJO -> (int) (Double.parseDouble(logPOJO.agent_id) * 1))
                //(1) Keyed event time tumbling window
                //.window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //(2) Keyed event time sliding window
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                //(3) Keyed event time session window
                //.window(EventTimeSessionWindows.withGap(Time.seconds(2)))
                .aggregate(new AggregateFunction<LogPOJO, String, String>() {

                    @Override
                    public String createAccumulator() {
                        return "";
                    }

                    @Override
                    public String add(LogPOJO logPOJO, String s) {
                        return s + logPOJO.activity;
                    }

                    @Override
                    public String getResult(String s) {
                        return s;
                    }

                    @Override
                    public String merge(String s, String acc1) {
                        return null;
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
