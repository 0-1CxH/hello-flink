package com.learnFlink.dataTransformsTestDrive;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.logging.Filter;

public class filterTransform {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<LogPOJO> stream = env.fromCollection(new prepareData().get());
        System.out.println(new prepareData().get());

        SingleOutputStreamOperator<LogPOJO> fs_1 = stream.filter(new filterActivity());
        SingleOutputStreamOperator<LogPOJO> fs_2 = stream.filter(t -> t.activity.equals("process_exit"));
        SingleOutputStreamOperator<LogPOJO> fs_3 = stream.filter(
                new FilterFunction<LogPOJO>() {
                    @Override
                    public boolean filter(LogPOJO logPOJO) throws Exception {
                        return logPOJO.activity.equals("process_create");
                    }
                }
        );

        fs_1.print("INNER_CLASS");
        fs_2.print("LAMBDA_FUNCTION");
        fs_3.print("ANONYMOUS_CLASS");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static class filterActivity implements FilterFunction<LogPOJO>{
        @Override
        public boolean filter(LogPOJO logPOJO) throws Exception {
            return logPOJO.activity.equals("process_create");
        }
    }
}
