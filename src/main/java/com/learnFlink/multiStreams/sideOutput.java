package com.learnFlink.multiStreams;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class sideOutput {
    public static void main(String[] args) throws Exception {
        OutputTag<Integer> count = new OutputTag<Integer>("count"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<LogPOJO> stream = env.addSource(new selfDefineLogPojoSource())
                .process(new ProcessFunction<LogPOJO, LogPOJO>() {

                    @Override
                    public void processElement(LogPOJO logPOJO, ProcessFunction<LogPOJO, LogPOJO>.Context context, Collector<LogPOJO> collector) throws Exception {
                        collector.collect(logPOJO);
                        context.output(count, 1);
                    }
                });

        stream.print();
        stream.getSideOutput(count).print();


        env.execute();



    }
}
