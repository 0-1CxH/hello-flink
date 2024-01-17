package com.learnFlink.dataTransformsTestDrive;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flatMapTransform {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<LogPOJO> stream = env.fromCollection(new prepareData().get());
        SingleOutputStreamOperator<String> flatten_stream = stream.flatMap(new flatMapFields());
        flatten_stream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class flatMapFields implements FlatMapFunction<LogPOJO, String>{
        @Override
        public void flatMap(LogPOJO logPOJO, Collector<String> collector) throws Exception {
            collector.collect(logPOJO.activity);
            collector.collect(logPOJO.agent_id);
            collector.collect(logPOJO.process_global_uid);
            collector.collect(logPOJO.unified_time.toString());
        }
    }
}
