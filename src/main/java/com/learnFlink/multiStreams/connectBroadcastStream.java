package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

public class connectBroadcastStream {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LogPOJO> dStream = env.addSource(new selfDefineLogPojoSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<LogPOJO>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<LogPOJO>) (d, l) -> d.unified_time.longValue()));

        SingleOutputStreamOperator<SwitchInfoPojo> ruleStream = env.addSource(new selfDefineSwitchPojoSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<SwitchInfoPojo>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<SwitchInfoPojo>) (i, l) -> i.timestamp));

        MapStateDescriptor<Void, Boolean> mapDes = new MapStateDescriptor<Void, Boolean>("switch", Void.class, Boolean.class);

        dStream
                .keyBy(x -> x.agent_id)
                .connect(ruleStream.broadcast(mapDes))
                .process(new KeyedBroadcastProcessFunction<String, LogPOJO, SwitchInfoPojo, LogPOJO>() {
                    final MapStateDescriptor<Void, Boolean> mapDes = new MapStateDescriptor<Void, Boolean>("switch", Void.class, Boolean.class);
                    @Override
                    public void processElement(LogPOJO logPOJO, KeyedBroadcastProcessFunction<String, LogPOJO, SwitchInfoPojo, LogPOJO>.ReadOnlyContext readOnlyContext, Collector<LogPOJO> collector) throws Exception {
                        Boolean s = readOnlyContext.getBroadcastState(mapDes).get(null);
                        if(s==null){
                            s = true;
                        }
                        if(s){
                            collector.collect(logPOJO);
                        }
                    }

                    @Override
                    public void processBroadcastElement(SwitchInfoPojo switchInfoPojo, KeyedBroadcastProcessFunction<String, LogPOJO, SwitchInfoPojo, LogPOJO>.Context context, Collector<LogPOJO> collector) throws Exception {
                        context.getBroadcastState(mapDes).put(null, switchInfoPojo.isOpen);
                        collector.collect(new LogPOJO("broastcast", BigDecimal.ONE, "BC_in_stream", "0"));
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
