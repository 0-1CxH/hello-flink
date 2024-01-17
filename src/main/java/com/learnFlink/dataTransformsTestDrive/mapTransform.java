package com.learnFlink.dataTransformsTestDrive;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

public class mapTransform {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LogPOJO> stream = env.fromCollection(new prepareData().get());
        SingleOutputStreamOperator<String> activity_stream = stream.map(new getActivity());
        SingleOutputStreamOperator<String> agent_id_stream = stream.map(t -> t.agent_id);
        SingleOutputStreamOperator<BigDecimal> time_stream = stream.map(
                new MapFunction<LogPOJO, BigDecimal>() {
                    @Override
                    public BigDecimal map(LogPOJO logPOJO) throws Exception {
                        return logPOJO.unified_time;
                    }
                }
        );

        activity_stream.print("INNER_CLASS");
        agent_id_stream.print("LAMBDA_FUNCTION");
        time_stream.print("ANONYMOUS_CLASS");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class getActivity implements MapFunction<LogPOJO, String>{
        @Override
        public String map(LogPOJO logPOJO) throws Exception {
            return logPOJO.activity;
        }
    }
}
