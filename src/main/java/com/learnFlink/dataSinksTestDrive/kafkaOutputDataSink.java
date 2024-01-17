package com.learnFlink.dataSinksTestDrive;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class kafkaOutputDataSink {
    public static void main(String[] args) {
        // get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read data from file
        DataStreamSource<String> text_file_stream = env.readTextFile("input/sample.txt");

        Properties kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers", "localhost:9092");

        text_file_stream.addSink(
                new FlinkKafkaProducer<String>(
                        "topic_two",
                        new SimpleStringSchema(),
                        kafkaProp
                )
        );

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
