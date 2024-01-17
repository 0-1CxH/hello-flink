package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.OrderInfoPojo;
import com.learnFlink.dataSourcesTestDrive.detectorPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class intervalJoinStreams {
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

        // S1(key=10001):    _____10001@0L__________     ________10001@100L____
        // S2(key=10001):    |            10001@20L |    | 10001@80L          |
        // IntervalJoin:    [(0-50, 0+50) call 1*1 join]    [(100-50. 100+50) call 1*1 join]
        // S1(key=10002):    ___10002@200L______   __________________10002@290L__
        // S2(key=10002):    |                  |  | 10002@250L 10002@280L       |
        // IntervalJoin:    [(200-50, 200+50) call 1*0 join]    [(290-50. 290+50) call 1*2 join]
        stream1.keyBy(x->x.detectorID)
                .intervalJoin(stream2.keyBy(y -> y.detectorID))
                .between(Time.milliseconds(-50), Time.milliseconds(50))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<detectorPojo, detectorPojo, String>() {
                    @Override
                    public void processElement(detectorPojo detectorPojo, detectorPojo detectorPojo2, ProcessJoinFunction<detectorPojo, detectorPojo, String>.Context context, Collector<String> collector) throws Exception {
                        if(detectorPojo.alarm && detectorPojo2.alarm){
                            int s = detectorPojo.value + detectorPojo2.value;
                            collector.collect(String.format("Alarm: %s, Sum: %d", detectorPojo.detectorID, s));
                        }
                        else{
                            collector.collect(String.format("False Alarm: %s", detectorPojo.detectorID));
                        }
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
