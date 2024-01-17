package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.OrderInfoPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class billCheckExampleWithIntervalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<OrderInfoPojo> stream1 = env.fromElements(
                new OrderInfoPojo(10001L, 120.85, true, "client", 0L),
                new OrderInfoPojo(10002L, 5613.06, true, "client", 1000L),
                new OrderInfoPojo(10003L, 450.55, true, "client", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfoPojo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner((SerializableTimestampAssigner<OrderInfoPojo>) (orderInfoPojo, l) -> orderInfoPojo.timestamp));


        SingleOutputStreamOperator<OrderInfoPojo> stream2 = env.fromElements(
                new OrderInfoPojo(10001L, 120.85, true, "platform", 200L),
                new OrderInfoPojo(10002L, 5613.06, true, "platform", 800L),
                new OrderInfoPojo(10004L, 560.08, true, "platform", 1500L),
                new OrderInfoPojo(10009L, 42.01, true, "platform", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfoPojo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner((SerializableTimestampAssigner<OrderInfoPojo>) (orderInfoPojo, l) -> orderInfoPojo.timestamp));

        stream1.keyBy(x -> x.orderID)
                .intervalJoin(stream2.keyBy(y -> y.orderID))
                .between(Time.milliseconds(-500), Time.milliseconds(500))
                .process(new ProcessJoinFunction<OrderInfoPojo, OrderInfoPojo, String>() {

                    @Override
                    public void processElement(OrderInfoPojo orderInfoPojo, OrderInfoPojo orderInfoPojo2, ProcessJoinFunction<OrderInfoPojo, OrderInfoPojo, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(String.format("%s is Paired %d %d", orderInfoPojo.orderID, orderInfoPojo.timestamp, orderInfoPojo2.timestamp));
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
