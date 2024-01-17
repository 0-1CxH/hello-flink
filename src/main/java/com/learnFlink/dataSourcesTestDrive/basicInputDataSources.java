package com.learnFlink.dataSourcesTestDrive;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.util.ArrayList;

public class basicInputDataSources {
    public static void main(String[] args) {
        // get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read data from file
        DataStreamSource<String> text_file_stream = env.readTextFile("input/sample_logs.json");

        // read from collection in memory
        LogPOJO elem_1 = new LogPOJO(
                "process_create",
                new BigDecimal("1645327532920"),
                "660fa076-cb11-41e7-b855-c6b3f8b0cd3c",
                "6152873b378feae2");
        LogPOJO elem_2 = new LogPOJO(
                "process_exit",
                new BigDecimal("1645327532921"),
                "266a608b-ad53-4b64-a3b9-ca1889c8cac3",
                "6152873b378feae2"
        );
        ArrayList<LogPOJO> log_list = new ArrayList<>();
        log_list.add(elem_1);
        log_list.add(elem_2);

        DataStreamSource<LogPOJO> collection_stream = env.fromCollection(log_list);

        // read from elements in memory
        DataStreamSource<LogPOJO> elements_stream = env.fromElements(elem_1, elem_2);

        DataStreamSource<LogPOJO> self_defined_stream = env.addSource(new selfDefineLogPojoSource());


        //sink
        text_file_stream.print("TEXT_FILE");
        collection_stream.print("COLLECTION");
        elements_stream.print("ELEMENT");
        self_defined_stream.print("DEFINE");

        //execute
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
