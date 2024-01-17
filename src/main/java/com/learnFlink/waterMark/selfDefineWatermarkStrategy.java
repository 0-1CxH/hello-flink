package com.learnFlink.waterMark;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class selfDefineWatermarkStrategy {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000);
        env
                .addSource(new selfDefineLogPojoSource())
                //.setParallelism(1)
                .assignTimestampsAndWatermarks(new WatermarkStrategy<LogPOJO>() {
                    @Override
                    public WatermarkGenerator<LogPOJO> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<LogPOJO>() {
                            private long seen_max_timestamp = Long.MIN_VALUE;

                            @Override
                            public void onEvent(LogPOJO logPOJO, long l, WatermarkOutput watermarkOutput) {
                                seen_max_timestamp = Math.max(seen_max_timestamp, l);
                                System.out.println("On Event: Update Seen Max Timestamp to "+seen_max_timestamp);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                long delay = 2000;
                                watermarkOutput.emitWatermark(new Watermark(seen_max_timestamp- delay -1));
                                System.out.println("OnPeriodic: Emit Watermark "+ (seen_max_timestamp- delay -1));
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<LogPOJO> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new SerializableTimestampAssigner<LogPOJO>() {
                            @Override
                            public long extractTimestamp(LogPOJO logPOJO, long l) {
                                return Long.parseLong(String.valueOf(logPOJO.unified_time));
                            }
                        };
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
