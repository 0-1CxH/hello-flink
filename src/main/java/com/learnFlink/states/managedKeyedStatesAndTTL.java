package com.learnFlink.states;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.learnFlink.dataSourcesTestDrive.OrderInfoPojo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class managedKeyedStatesAndTTL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StateTtlConfig TtlConf = StateTtlConfig
                .newBuilder(Time.milliseconds(200))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        SingleOutputStreamOperator<OrderInfoPojo> stream = env.fromElements(
                new OrderInfoPojo(10001L, 120.85, true, "client", 0L),
                new OrderInfoPojo(10001L, 120.85, true, "platform", 200L),
                new OrderInfoPojo(10002L, 5613.06, true, "platform", 700L),
                new OrderInfoPojo(10002L, 5613.06, true, "client", 1000L),
                new OrderInfoPojo(10004L, 560.08, true, "platform", 1500L),
                new OrderInfoPojo(10003L, 450.55, true, "client", 2000L),
                new OrderInfoPojo(10009L, 42.01, true, "platform", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfoPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<OrderInfoPojo>) (orderInfoPojo, l) -> orderInfoPojo.timestamp));

        stream.keyBy(x -> x.orderID)
                .process(new KeyedProcessFunction<Long, OrderInfoPojo, String>() {
                    ValueState<Integer> recordsTotal;
                    ListState<Long> timestampsList;
                    MapState<String, Double> sourceAndPmt;
                    ReducingState<OrderInfoPojo> latestInfo;
                    AggregatingState<OrderInfoPojo, JSONObject> totalInfo;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Integer> valDes = new ValueStateDescriptor<Integer>("rec_total", Integer.class);
                        recordsTotal = getRuntimeContext().getState(valDes);

                        ListStateDescriptor<Long> lstDes = new ListStateDescriptor<Long>("ts_list", Long.class);
                        lstDes.enableTimeToLive(TtlConf);
                        timestampsList = getRuntimeContext().getListState(lstDes);


                        MapStateDescriptor<String, Double> mapDes = new MapStateDescriptor<String, Double>("src_pmt_map", String.class, Double.class);
                        //mapDes.enableTimeToLive(TtlConf);
                        sourceAndPmt = getRuntimeContext().getMapState(mapDes);

                        ReducingStateDescriptor<OrderInfoPojo> redDes = new ReducingStateDescriptor<OrderInfoPojo>("latest_info", new ReduceFunction<OrderInfoPojo>() {
                            @Override
                            public OrderInfoPojo reduce(OrderInfoPojo orderInfoPojo, OrderInfoPojo t1) throws Exception {
                                orderInfoPojo.timestamp = Math.max(orderInfoPojo.timestamp, t1.timestamp);
                                orderInfoPojo.payment += t1.payment;
                                return orderInfoPojo;
                            }
                        }, OrderInfoPojo.class);
                        latestInfo = getRuntimeContext().getReducingState(redDes);

                        AggregatingStateDescriptor<OrderInfoPojo, JSONObject, JSONObject> aggDes = new AggregatingStateDescriptor<OrderInfoPojo, JSONObject, JSONObject>("total_info", new AggregateFunction<OrderInfoPojo, JSONObject, JSONObject>() {
                            @Override
                            public JSONObject createAccumulator() {
                                return new JSONObject().fluentPut("paymentSum", 0).fluentPut("timeStamps", new JSONArray()).fluentPut("sourceArray", new JSONArray());
                            }

                            @Override
                            public JSONObject add(OrderInfoPojo orderInfoPojo, JSONObject jsonObject) {
                                return jsonObject.fluentPut("paymentSum", jsonObject.getDoubleValue("paymentSum") + orderInfoPojo.payment)
                                        .fluentPut("timeStamps", jsonObject.getJSONArray("timeStamps").fluentAdd(orderInfoPojo.timestamp))
                                        .fluentPut("sourceArray", jsonObject.getJSONArray("sourceArray").fluentAdd(orderInfoPojo.source));
                            }

                            @Override
                            public JSONObject getResult(JSONObject jsonObject) {
                                return jsonObject;
                            }

                            @Override
                            public JSONObject merge(JSONObject jsonObject, JSONObject acc1) {
                                return null;
                            }
                        }, JSONObject.class);
                        totalInfo = getRuntimeContext().getAggregatingState(aggDes);

                    }

                    @Override
                    public void processElement(OrderInfoPojo orderInfoPojo, KeyedProcessFunction<Long, OrderInfoPojo, String>.Context context, Collector<String> collector) throws Exception {
                        if(recordsTotal.value()==null){
                            recordsTotal.update(0);
                        }
                        recordsTotal.update(recordsTotal.value()+1);
                        timestampsList.add(orderInfoPojo.timestamp);
                        sourceAndPmt.put(orderInfoPojo.source, orderInfoPojo.payment);
                        latestInfo.add(orderInfoPojo);
                        totalInfo.add(orderInfoPojo);

                        System.out.printf("At arrival of %s, Key[%s]'s total records=%d, timestamps=%s%n, sources/payments=%s, latest=%s, total=%s\n",
                                orderInfoPojo, context.getCurrentKey(),recordsTotal.value(),timestampsList.get(), sourceAndPmt.entries(),
                                latestInfo.get(), totalInfo.get()
                        );

                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
