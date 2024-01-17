package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.detectorPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class windowCogroupStreams {
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

        // Sliding(100L, 50L)    |--------------- 0-99 ------| | -------- 100-199 ---| |---------  200-299 -------------|
        //              |-----  -50-49 -----|  |------------- 50-149 ----| |-------150-249 -------| |----------250-349 -----|
        // S1(key=10001):         10001@0L                 10001@100L
        // S2(key=10001):                10001@20L    10001@80L
        // window(key=10001):       [call 1 coGroup()]              [call 1 coGroup()]
        // window(key=10001):   [call 1 coGroup()]     [call 1 coGroup()]
        // S1(key=10002):                                                                  10002@200L              10002@290L
        // S2(key=10002):                                                                      10002@250L       10002@280L
        // window(key=10002):                                                        [call 1 coGroup()]      [call 1 coGroup()]
        // window(key=10002):                                                                      [call 1 coGroup()]
        stream1.coGroup(stream2).where(x -> x.detectorID).equalTo(y -> y.detectorID)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(100L), Time.milliseconds(50L)))
                .apply(new CoGroupFunction<detectorPojo, detectorPojo, String>() {

                    @Override
                    public void coGroup(Iterable<detectorPojo> iterable, Iterable<detectorPojo> iterable1, Collector<String> collector) throws Exception {

                        StringBuilder s = new StringBuilder();
                        s.append("S1 elements=");
                        for(detectorPojo d: iterable){
                            s.append(d.detectorID).append("@").append(d.timestamp).append(" ");
                        }
                        s.append(", S2 elements=");
                        for(detectorPojo d: iterable1){
                            s.append(d.detectorID).append("@").append(d.timestamp).append(" ");
                        }
                        collector.collect(s.toString());
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
