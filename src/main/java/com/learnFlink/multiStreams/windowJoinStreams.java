package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.OrderInfoPojo;
import com.learnFlink.dataSourcesTestDrive.detectorPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class windowJoinStreams {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<detectorPojo> stream1 = env.fromElements(
                new detectorPojo("10001", false, 50, 0L),
                new detectorPojo("10001", true, 60, 100L),
                new detectorPojo("10002", false, 40, 200L),
                new detectorPojo("10002", true, 55, 290L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<detectorPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<detectorPojo>) (d, l) -> d.timestamp));


        SingleOutputStreamOperator<detectorPojo> stream2 = env.fromElements(
                new detectorPojo("10001", true, 55, 20L),
                new detectorPojo("10001", true, 70, 80L),
                new detectorPojo("10002", true, 55, 250L),
                new detectorPojo("10002", true, 55, 280L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<detectorPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<detectorPojo>) (d, l) -> d.timestamp));

        // Tumbling(150L)    |------ 0-149 -------|     |------ 150-299 -----|
        // S1(key=10001):      10001@0L 10001@100L
        // S2(key=10001):      10001@20L 10001@80L
        // window(key=10001):  [call 2*2=4 join()]
        // S1(key=10002):                                10002@200L 10002@290L
        // S2(key=10002):                                10002@250L 10002@280L
        // window(key=10002):                             [call 2*2=4 join()]
        stream1.join(stream2).where(x -> x.detectorID).equalTo(y -> y.detectorID)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(150L)))
                .apply(new JoinFunction<detectorPojo, detectorPojo, String>() {
                    @Override
                    public String join(detectorPojo detectorPojo, detectorPojo detectorPojo2) throws Exception {
                        if(detectorPojo.alarm && detectorPojo2.alarm){
                            int s = detectorPojo.value + detectorPojo2.value;
                            return String.format("Alarm: %s, Sum: %d", detectorPojo.detectorID, s );
                        }
                        else{
                            return String.format("False Alarm: %s", detectorPojo.detectorID);
                        }
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
