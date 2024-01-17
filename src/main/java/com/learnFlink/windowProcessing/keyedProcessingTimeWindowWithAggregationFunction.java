package com.learnFlink.windowProcessing;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class keyedProcessingTimeWindowWithAggregationFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new selfDefineLogPojoSource())
                .setParallelism(1)
                .keyBy((KeySelector<LogPOJO, Object>) logPOJO -> (int) (Double.parseDouble(logPOJO.agent_id) * 1))
                //(1) Keyed processing time tumbling window
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                //(2) Keyed processing time sliding window
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                //(3) Keyed processing time session window
                //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .process(new ProcessWindowFunction<LogPOJO, Object, Object, TimeWindow>() {

                    @Override
                    public void process(Object o, ProcessWindowFunction<LogPOJO, Object, Object, TimeWindow>.Context context, Iterable<LogPOJO> iterable, Collector<Object> collector) throws Exception {
                        Iterator<LogPOJO> iter = iterable.iterator();
                        List<LogPOJO> lst = new ArrayList<>();
                        while(iter.hasNext()){
                            lst.add(iter.next());
                        }
                        collector.collect(lst.toString() + context.currentWatermark()); // print watermark
                        
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
