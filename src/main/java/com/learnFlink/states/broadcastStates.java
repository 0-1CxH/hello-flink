package com.learnFlink.states;

import com.learnFlink.dataSourcesTestDrive.AllowListInfoPojo;
import com.learnFlink.dataSourcesTestDrive.OrderInfoPojo;
import com.learnFlink.dataSourcesTestDrive.SwitchInfoPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.reflect.internal.Types;

import java.util.*;

public class broadcastStates {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderInfoPojo> orderStream = env.fromElements(
                new OrderInfoPojo(10001L, 120.85, true, "client", 0L),
                new OrderInfoPojo(10001L, 120.85, true, "platform", 200L),
                new OrderInfoPojo(10002L, 5613.06, true, "platform", 700L),
                new OrderInfoPojo(10002L, 5613.06, true, "client", 1000L),
                new OrderInfoPojo(10004L, 560.08, true, "platform", 1500L),
                new OrderInfoPojo(10003L, 450.55, true, "client", 2000L),
                new OrderInfoPojo(10009L, 42.01, true, "platform", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfoPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<OrderInfoPojo>) (orderInfoPojo, l) -> orderInfoPojo.timestamp));

        SingleOutputStreamOperator<SwitchInfoPojo> switchStream = env.fromElements(
                new SwitchInfoPojo(0L, true),
                new SwitchInfoPojo(300L, false),
                new SwitchInfoPojo(900L, true),
                new SwitchInfoPojo(1200L, false),
                new SwitchInfoPojo(1800L, true)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<SwitchInfoPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<SwitchInfoPojo>) (i, l) -> i.timestamp));

        SingleOutputStreamOperator<AllowListInfoPojo>  allowStream = env.fromElements(
                new AllowListInfoPojo(10001L, true, 0L),
                new AllowListInfoPojo(10002L, true, 0L),
                new AllowListInfoPojo(10003L, false, 0L),
                new AllowListInfoPojo(10004L, true, 100L),
                new AllowListInfoPojo(10009L, false, 500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<AllowListInfoPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<AllowListInfoPojo>) (allowListInfoPojo, l) -> allowListInfoPojo.timestamp));


        MapStateDescriptor<Void, Boolean> switchDes = new MapStateDescriptor<Void, Boolean>("switch", Void.class, Boolean.class); // make it act like boolean value state
        MapStateDescriptor<Long, Void> allowDes = new MapStateDescriptor<Long, Void>("allow", Long.class, Void.class); // make it act like hashset


        BroadcastStream<SwitchInfoPojo> bcSwitch = switchStream.broadcast(switchDes);
        BroadcastStream<AllowListInfoPojo> bcAllow = allowStream.broadcast(allowDes);

        orderStream
                .connect(bcSwitch)
                .process(
                new BroadcastProcessFunction<OrderInfoPojo, SwitchInfoPojo, OrderInfoPojo>() {

                    final MapStateDescriptor<String, Boolean> bcDes = new MapStateDescriptor<String, Boolean>("switch", String.class, Boolean.class);

                    @Override
                    public void processElement(OrderInfoPojo orderInfoPojo, BroadcastProcessFunction<OrderInfoPojo, SwitchInfoPojo, OrderInfoPojo>.ReadOnlyContext readOnlyContext, Collector<OrderInfoPojo> collector) throws Exception {
                        ReadOnlyBroadcastState<String, Boolean> s = readOnlyContext.getBroadcastState(bcDes);
                        Boolean isOpening = s.get(null);
                        if(isOpening==null){
                            isOpening = true;
                        }
                        if(isOpening){
                            collector.collect(orderInfoPojo);
                        }
                    }

                    @Override
                    public void processBroadcastElement(SwitchInfoPojo switchInfoPojo, BroadcastProcessFunction<OrderInfoPojo, SwitchInfoPojo, OrderInfoPojo>.Context context, Collector<OrderInfoPojo> collector) throws Exception {
                        BroadcastState<String, Boolean> s = context.getBroadcastState(bcDes);
                        s.put(null, switchInfoPojo.isOpen);
                    }
                }

        )
                .connect(bcAllow)
                .process(new BroadcastProcessFunction<OrderInfoPojo, AllowListInfoPojo, OrderInfoPojo>() {

                    final MapStateDescriptor<Long, Void> bcDes = new MapStateDescriptor<Long, Void>("allow", Long.class, Void.class);

                    @Override
                    public void processElement(OrderInfoPojo orderInfoPojo, BroadcastProcessFunction<OrderInfoPojo, AllowListInfoPojo, OrderInfoPojo>.ReadOnlyContext readOnlyContext, Collector<OrderInfoPojo> collector) throws Exception {
                        ReadOnlyBroadcastState<Long, Void> s = readOnlyContext.getBroadcastState(bcDes);
                        if(s.contains(orderInfoPojo.orderID)){
                            collector.collect(orderInfoPojo);
                        }
                    }

                    @Override
                    public void processBroadcastElement(AllowListInfoPojo allowListInfoPojo, BroadcastProcessFunction<OrderInfoPojo, AllowListInfoPojo, OrderInfoPojo>.Context context, Collector<OrderInfoPojo> collector) throws Exception {
                        BroadcastState<Long, Void> s = context.getBroadcastState(bcDes);
                        long id = allowListInfoPojo.id;
                        if(allowListInfoPojo.op){
                            if(!s.contains(id)){
                                s.put(id, null);
                            }
                        }
                        else{
                            if(s.contains(id)){
                                s.remove(id);
                            }
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
