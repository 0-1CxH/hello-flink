package com.learnFlink.windowProcessing;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;


public class keyedCountWindowAndKeyedGlobalWindowWithAggregationAndProcessWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new selfDefineLogPojoSource())
                .setParallelism(1)
                .keyBy((KeySelector<LogPOJO, Object>) logPOJO -> (int) (Double.parseDouble(logPOJO.agent_id) * 1))
                //(1) Keyed count tumbling window
                //.countWindow(5)
                //(2) Keyed count sliding window
                .countWindow(5, 3)
                //(3) Keyed global window
                //.window(GlobalWindows.create())
                .aggregate(new AggregateFunction<LogPOJO, String, String>() {

                               @Override
                               public String createAccumulator() {
                                   return "";
                               }

                               @Override
                               public String add(LogPOJO logPOJO, String s) {
                                   return s + logPOJO.agent_id;
                               }

                               @Override
                               public String getResult(String s) {
                                   return s;
                               }

                               @Override
                               public String merge(String s, String acc1) {
                                   return null;
                               }
                           },
                        new ProcessWindowFunction<String, Object, Object, GlobalWindow>() {

                            @Override
                            public void process(Object o, ProcessWindowFunction<String, Object, Object, GlobalWindow>.Context context, Iterable<String> iterable, Collector<Object> collector) throws Exception {
                                collector.collect(iterable.iterator().next().length() + "@" +context.currentWatermark());
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
