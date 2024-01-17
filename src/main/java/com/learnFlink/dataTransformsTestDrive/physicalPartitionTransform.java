package com.learnFlink.dataTransformsTestDrive;


import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class physicalPartitionTransform {
    public static void main(String[] args) throws Exception {
        ArrayList<Integer> r = new ArrayList<>();
        for(int i=0; i<20; i++){
            r.add(i);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numbers_stream = env.fromCollection(r).setParallelism(1);

        numbers_stream.print("DEFAULT").setParallelism(2); // default = rebalance
        numbers_stream.shuffle().print("SHUFFLE").setParallelism(2);
        numbers_stream.rebalance().print("REBALANCE").setParallelism(4);
        numbers_stream.global().print("GLOBAL").setParallelism(2);
        numbers_stream.rescale().print("RESCALE").setParallelism(4);
        numbers_stream.broadcast().print("BROADCAST").setParallelism(3);

        numbers_stream.partitionCustom(
                (Partitioner<Integer>) (integer, numPartitions) -> integer % numPartitions,
                t -> t
        ).print("CUSTOM_LAMBDA").setParallelism(3);

        numbers_stream.partitionCustom(
                new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer integer, int numPartitions) {
                        return integer % numPartitions;
                    }
                },
                new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer integer) throws Exception {
                        return integer;
                    }
                }
        ).print("CUSTOM_ANONYMOUS_CLASS").setParallelism(5);


        env.execute();

    }

}
