package com.learnFlink.processFunctions;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class processWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<LogPOJO, Boolean, TimeWindow> afterWindowed = env
                .addSource(new selfDefineLogPojoSource())
                .assignTimestampsAndWatermarks((WatermarkStrategy<LogPOJO>) context -> new keyedProcessFunction.CountWaterMarkGen())
                .keyBy(x -> true)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2L)));

        afterWindowed.process(new ProcessWindowFunction<LogPOJO, Object, Boolean, TimeWindow>() {
            @Override
            public void process(Boolean aBoolean, ProcessWindowFunction<LogPOJO, Object, Boolean, TimeWindow>.Context context, Iterable<LogPOJO> iterable, Collector<Object> collector) throws Exception {
                long windowStart = context.window().getStart();
                long windowEnd = context.window().getEnd();
                long watermark = context.currentWatermark();
                KeyedStateStore winState = context.windowState();
                KeyedStateStore globState = context.globalState();
                boolean currentKey = aBoolean;
                List<LogPOJO> elements = new ArrayList<>();
                for (LogPOJO logPOJO : iterable) {
                    elements.add(logPOJO);
                }

                String s = String.format("Window Key=%s, Start=%s, End=%s, Watermark=%s, WindowState=%s, GlobalState=%s, Window Elements=%s",
                        currentKey, windowStart, windowEnd, watermark, winState, globState, elements);
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
