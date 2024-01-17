package com.learnFlink.wordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class remoteSocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment remote_env = StreamExecutionEnvironment.createRemoteEnvironment(
                "localhost", 8081
        );
//        DataStreamSource<String> txt_stream_source = remote_env.socketTextStream("172.26.0.4",7777);
//
//        SingleOutputStreamOperator<Tuple2<String, Long>> word_and_1L =
//                txt_stream_source.flatMap(
//                        (String input_line, Collector<Tuple2<String, Long>> output_tuples_collector) ->
//                        {
//                            String[] split_line = input_line.split(" ");
//                            for(String word: split_line){
//                                output_tuples_collector.collect(Tuple2.of(word, 1L));
//                            }
//                        }
//                ).returns(Types.TUPLE(Types.STRING, Types.LONG));
//        KeyedStream<Tuple2<String, Long>, String> word_and_count = word_and_1L.keyBy(t -> t.f0);
//        SingleOutputStreamOperator<Tuple2<String, Long>> result = word_and_count.sum(1);
//        result.print();
//
//        remote_env.execute();

    }
}
