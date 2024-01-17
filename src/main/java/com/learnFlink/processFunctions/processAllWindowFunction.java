package com.learnFlink.processFunctions;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class processAllWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AllWindowedStream<LogPOJO, TimeWindow> afterAllWindowed = env
                .addSource(new selfDefineLogPojoSource())
                .assignTimestampsAndWatermarks((WatermarkStrategy<LogPOJO>) context -> new keyedProcessFunction.CountWaterMarkGen())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2L)));

        afterAllWindowed.process(new ProcessAllWindowFunction<LogPOJO, Object, TimeWindow>() {

            @Override
            public void process(ProcessAllWindowFunction<LogPOJO, Object, TimeWindow>.Context context, Iterable<LogPOJO> iterable, Collector<Object> collector) throws Exception {
                long windowStart = context.window().getStart();
                long windowEnd = context.window().getEnd();
                KeyedStateStore winState = context.windowState();
                KeyedStateStore globState = context.globalState();
                List<LogPOJO> elements = new ArrayList<>();
                for (LogPOJO logPOJO : iterable) {
                    elements.add(logPOJO);
                }

                String s = String.format("Window Start=%s, End=%s, WindowState=%s, GlobalState=%s, Window Elements=%s",
                        windowStart, windowEnd, winState, globState, elements);
                collector.collect(s);
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
