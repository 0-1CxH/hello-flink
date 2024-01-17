package com.learnFlink.dataTransformsTestDrive;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class aggTransform {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<LogPOJO> stream = env.fromCollection(new prepareData().get());

        SingleOutputStreamOperator<Tuple3<String, String, Long>> map_stream
                = stream.map(t -> Tuple3.of(t.activity, t.agent_id, 1L))
                .returns(new TypeHint<Tuple3<String, String ,Long>>(){});

        KeyedStream<Tuple3<String, String, Long>, Tuple2<String, String>> keyed_map_stream
                = map_stream.keyBy(new KeySelector<Tuple3<String, String, Long>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                return Tuple2.of(stringStringLongTuple3.f0, stringStringLongTuple3.f1);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Long>> reduced = keyed_map_stream.reduce(
                new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t0, Tuple3<String, String, Long> t1) throws Exception {
                        return Tuple3.of(t0.f0, t0.f1, t0.f2 + t1.f2);
                    }
                }
        );

        reduced.print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
