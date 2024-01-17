package com.learnFlink.wordCount;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class socketStreamWordCount {
    public static void main(String[] args) {

        StreamExecutionEnvironment s_env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool param = ParameterTool.fromArgs(args);
        String host = param.get("host");
        String port = param.get("port");
        System.out.println(host);
        System.out.println(port);

        DataStreamSource<String> txt_stream_source = s_env.socketTextStream(host, Integer.parseInt(port));

        SingleOutputStreamOperator<Tuple2<String, Long>> word_and_1L =
                txt_stream_source.flatMap(
                        (String input_line, Collector<Tuple2<String, Long>> output_tuples_collector) ->
                        {
                            String[] split_line = input_line.split(" ");
                            for(String word: split_line){
                                output_tuples_collector.collect(Tuple2.of(word, 1L));
                            }
                        }
                ).returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> word_and_count = word_and_1L.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> result = word_and_count.sum(1);

        result.print();


        try{
            s_env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
