package com.learnFlink.processFunctions;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class basicProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new selfDefineLogPojoSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LogPOJO>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<LogPOJO>) (logPOJO, l) -> logPOJO.unified_time.longValue()))
                .process(new ProcessFunction<LogPOJO, Object>() {

                    @Override
                    public void processElement(LogPOJO logPOJO, ProcessFunction<LogPOJO, Object>.Context context, Collector<Object> collector) throws Exception {
                        long processingTime = context.timerService().currentProcessingTime(); // current processing time
                        long waterMark = context.timerService().currentWatermark(); // current watermark
                        long timestamp = context.timestamp(); // ts of element (>= watermark)

                        String s = String.format("On Element :: Element=%s, Watermark=%s, ProcessingTime=%s, Timestamp=%s" ,logPOJO, waterMark, processingTime, timestamp);
                        collector.collect(s);
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
