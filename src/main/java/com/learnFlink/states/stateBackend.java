package com.learnFlink.states;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class stateBackend {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setStateBackend(new HashMapStateBackend());

        env.setStateBackend(new EmbeddedRocksDBStateBackend()); // need dependency flink-statebackend-rocksdb_${scala.binary.version}



    }
}
