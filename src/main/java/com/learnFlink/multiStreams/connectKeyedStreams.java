package com.learnFlink.multiStreams;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class connectKeyedStreams {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 3, 5, 7);


        DataStreamSource<Long> stream2 = env.fromElements(100L, 200L, 600L, 700L);

        stream1.connect(stream2)
                .keyBy(x -> true, x -> true) // (stream1-connect-stream2).keyBy equals to (stream1.keyBy)-connect-(stream2.keyBy)
                .process(new CoProcessFunction<Integer, Long, String>() {
                    ValueState<Long> highestVal;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Long> valDes = new ValueStateDescriptor<Long>("m_val", Types.LONG);
                        highestVal = getRuntimeContext().getState(valDes);
                    }

                    @Override
                    public void processElement1(Integer integer, CoProcessFunction<Integer, Long, String>.Context context, Collector<String> collector) throws Exception {
                        if(highestVal.value()==null){
                            highestVal.update(0L);
                        }

                        long m = (long) integer * 100;
                        highestVal.update(Math.max(highestVal.value(), m));
                    }

                    @Override
                    public void processElement2(Long aLong, CoProcessFunction<Integer, Long, String>.Context context, Collector<String> collector) throws Exception {
                        if(highestVal.value()==null){
                            highestVal.update(0L);
                        }

                        String middle;
                        if(highestVal.value()>aLong){
                            middle = " > ";
                        }
                        else{
                            middle = " <= ";
                        }
                        collector.collect(highestVal.value() + middle + aLong);
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
