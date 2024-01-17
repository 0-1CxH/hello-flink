package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.OrderInfoPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;

public class billCheckExampleWithConnect {
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


        stream1.connect(stream2)
                .keyBy( orderInfoPojo -> orderInfoPojo.orderID, orderInfoPojo -> orderInfoPojo.orderID)
                .process(new CoProcessFunction<OrderInfoPojo, OrderInfoPojo, String>() {
                    ValueState<Boolean> notFinished;
                    ValueState<Long> orderID;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> bDes = new ValueStateDescriptor<Boolean>("is_finished", Boolean.class, false);
                        ValueStateDescriptor<Long> oDes = new ValueStateDescriptor<Long>("order_id", Long.class);
                        notFinished = getRuntimeContext().getState(bDes);
                        orderID = getRuntimeContext().getState(oDes);
                    }

                    @Override
                    public void processElement1(OrderInfoPojo orderInfoPojo, CoProcessFunction<OrderInfoPojo, OrderInfoPojo, String>.Context context, Collector<String> collector) throws Exception {
                        context.timerService().registerEventTimeTimer(context.timestamp() + 500L);
                        // System.out.println("1}" + orderInfoPojo.orderID +" Arrived, ts=" + context.timestamp() + ", wm=" + context.timerService().currentWatermark());
                        notFinished.update(!notFinished.value());
                        if(orderID.value()==null){
                            orderID.update(orderInfoPojo.orderID);
                        }
                    }

                    @Override
                    public void processElement2(OrderInfoPojo orderInfoPojo, CoProcessFunction<OrderInfoPojo, OrderInfoPojo, String>.Context context, Collector<String> collector) throws Exception {
                        context.timerService().registerEventTimeTimer(context.timestamp() + 500L);
                        // System.out.println("2}" + orderInfoPojo.orderID +" Arrived, ts=" + context.timestamp() + ", wm=" + context.timerService().currentWatermark());
                        notFinished.update(!notFinished.value());
                        if (orderID.value() == null) {
                            orderID.update(orderInfoPojo.orderID);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<OrderInfoPojo, OrderInfoPojo, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        // System.out.println("timer} " + " ts=" + ctx.timestamp() + ", wm=" + ctx.timerService().currentWatermark());
                        if(notFinished.value()){
                            out.collect("Cannot pair " + orderID.value());
                        }
                        notFinished.clear();

                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
