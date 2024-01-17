package com.learnFlink.windowProcessing;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class trigger {
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
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))

                .trigger(new SelfDefinedTrigger())
                //.trigger(ProcessingTimeTrigger.create())
                //.trigger(EventTimeTrigger.create())
                //.trigger(CountTrigger.of(5))
                .aggregate(new selfDefinedAggregation())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class SelfDefinedTrigger extends Trigger<LogPOJO, TimeWindow>{

        int startFiringCount = 10;
        int purgeWindowCount = 20;
        int currentCount = 0;


        @Override
        public TriggerResult onElement(LogPOJO logPOJO, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            currentCount += 1;
            System.out.println(currentCount);
            if(currentCount > purgeWindowCount){
                triggerContext.registerEventTimeTimer(triggerContext.getCurrentWatermark() + 2L);
                currentCount = 0;
                return TriggerResult.FIRE_AND_PURGE;
            }
            if(currentCount > startFiringCount){
                triggerContext.registerProcessingTimeTimer(triggerContext.getCurrentProcessingTime() + 2000L);
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("Processing Time Trigger: "+ triggerContext.getCurrentProcessingTime());
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("Event Time Trigger: "+ triggerContext.getCurrentProcessingTime());
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    static class selfDefinedAggregation implements AggregateFunction<LogPOJO, String, String> {

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
    }


}
