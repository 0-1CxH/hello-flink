package com.learnFlink.processFunctions;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class keyedProcessFunction {
    static long startTime = System.currentTimeMillis();

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<LogPOJO, Boolean> afterKeyBy = env
                .addSource(new selfDefineLogPojoSource())
                .assignTimestampsAndWatermarks((WatermarkStrategy<LogPOJO>) context -> new CountWaterMarkGen())
                .keyBy(x -> true);

        SingleOutputStreamOperator<String> afterKeyedProcess = afterKeyBy.process(new KeyedProcessFunction<Boolean, LogPOJO, String>() {


            @Override
            public void processElement(LogPOJO logPOJO, KeyedProcessFunction<Boolean, LogPOJO, String>.Context context, Collector<String> collector) throws Exception {
                long processingTime = context.timerService().currentProcessingTime();
                long waterMark = context.timerService().currentWatermark();
                Boolean currentKey = context.getCurrentKey(); // get key of this keyed stream

                String s = String.format("On Element :: Element=%s, Key=%s Watermark=%s, ProcessingTime=%s, Timestamp=%s" ,logPOJO, currentKey, waterMark, processingTime, context.timestamp());
                // timestamp never changes because there is no timestamp assigner, the watermark is directly generated
                collector.collect(s);
                context.timerService().registerProcessingTimeTimer(processingTime+10000L);
                context.timerService().registerEventTimeTimer(waterMark+2L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, LogPOJO, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                if (timestamp > keyedProcessFunction.startTime){
                    out.collect("On Processing Timer :: " + timestamp);
                }
                else{
                    out.collect("On Event Timer :: " + timestamp);
                }

            }
        });

        afterKeyedProcess.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class CountWaterMarkGen implements WatermarkGenerator<LogPOJO>{
        long count = 0;

        @Override
        public void onEvent(LogPOJO logPOJO, long l, WatermarkOutput watermarkOutput) {
            watermarkOutput.emitWatermark(new Watermark(count));
            count+= 1;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

        }
    }

}
