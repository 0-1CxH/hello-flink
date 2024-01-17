package com.learnFlink.states;

import com.learnFlink.dataSourcesTestDrive.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class managedOperatorStates {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000L); // enable the checkpointing / snapshot

        env
                .addSource(new selfDefineLogPojoSource())
                .addSink(new bufferedSink(10));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static class bufferedSink implements CheckpointedFunction, SinkFunction<LogPOJO> {

        ListState<LogPOJO> snapshotListState;
        List<LogPOJO> buffer = new ArrayList<>();
        int bufferSize;

        public bufferedSink(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            snapshotListState.clear();
            snapshotListState.addAll(buffer);
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ListStateDescriptor<LogPOJO> lstDes = new ListStateDescriptor<LogPOJO>("buffer", Types.POJO(LogPOJO.class));
            snapshotListState = functionInitializationContext.getOperatorStateStore().getUnionListState(lstDes);
            if(functionInitializationContext.isRestored()){
                for(LogPOJO e: snapshotListState.get()){
                    buffer.add(e);
                }
            }
        }


        @Override
        public void invoke(LogPOJO value, Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
            buffer.add(value);
            if(buffer.size()>=this.bufferSize){
                System.out.println("output invoked "+buffer.size());
                buffer.clear();
            }
        }
    }
}

