package com.learnFlink.waterMark;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.time.Duration;

public class builtInWatermarkStrategy {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new selfDefineLogPojoSource())
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<LogPOJO>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                                .withTimestampAssigner((SerializableTimestampAssigner<LogPOJO>) (logPOJO, l) -> Long.parseLong(String.valueOf(logPOJO.unified_time)))
//                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LogPOJO>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<LogPOJO>) (logPOJO, l) -> Long.parseLong(String.valueOf(logPOJO.unified_time)))
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
