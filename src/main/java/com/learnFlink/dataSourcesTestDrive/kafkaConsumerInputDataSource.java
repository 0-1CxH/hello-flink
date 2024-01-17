package com.learnFlink.dataSourcesTestDrive;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class kafkaConsumerInputDataSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafka_prop = new Properties();
        kafka_prop.setProperty("bootstrap.servers", "localhost:9092");
        kafka_prop.setProperty("group.id", "java_flink_consumer_test_1");
        kafka_prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafka_prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafka_prop.setProperty("auto.offset.reset", "latest");


        DataStreamSource<LogPOJO> kafka_stream = env.addSource(new FlinkKafkaConsumer<>(
                "topic_one",
                new LogPojoSchema(),
                kafka_prop
        ));

        kafka_stream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}


