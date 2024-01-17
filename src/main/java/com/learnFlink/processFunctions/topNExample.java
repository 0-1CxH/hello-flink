package com.learnFlink.processFunctions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class topNExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new selfDefineLogPojoSource())
                .assignTimestampsAndWatermarks((WatermarkStrategy<LogPOJO>) context -> new keyedProcessFunction.CountWaterMarkGen())
                .keyBy(x -> (int) (Double.parseDouble(x.agent_id) * 3))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2L)))
                .process(new ProcessWindowFunction<LogPOJO, JSONObject, Integer, TimeWindow>() {

                    @Override
                    public void process(Integer integer, ProcessWindowFunction<LogPOJO, JSONObject, Integer, TimeWindow>.Context context, Iterable<LogPOJO> iterable, Collector<JSONObject> collector) throws Exception {
                        int count = 0;
                        for (LogPOJO $ : iterable) {
                            count += 1;
                        }
                        JSONObject jsonObj = new JSONObject().fluentPut("key", integer).fluentPut("count", count).fluentPut("end", context.window().getEnd());
                        collector.collect(jsonObj);
                    }
                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//                    @Override
//                    public long extractTimestamp(JSONObject jsonObject, long l) {
//                        return jsonObject.getLong("end");
//                    }
//                }))
                .keyBy(new keyByWindowEnd())
                .process(new SortStatsProcessFunction())
                .print("Phase2");

// statsByKeyWindow.print("Phase1");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    static class SortStatsProcessFunction extends KeyedProcessFunction<Long, JSONObject, String> {
        ListState<JSONObject> statsList;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            statsList = getRuntimeContext().getListState(new ListStateDescriptor<JSONObject>("stats-list", Types.GENERIC(JSONObject.class)));
        }

        @Override
        public void processElement(JSONObject jsonObject, KeyedProcessFunction<Long, JSONObject, String>.Context context, Collector<String> collector) throws Exception {
            statsList.add(jsonObject);
            long wm = context.timerService().currentWatermark();
            context.timerService().registerEventTimeTimer( wm + 1L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, JSONObject, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            List<JSONObject> jsonList = new ArrayList<>();
            for(JSONObject j: statsList.get()){
                jsonList.add(j);
            }
            jsonList.sort(Comparator.comparingInt(o -> o.getInteger("count")));

            out.collect(jsonList. toString());
        }
    }
    static class keyByWindowEnd implements KeySelector<JSONObject, Long>{

        @Override
        public Long getKey(JSONObject jsonObject) throws Exception {
            return jsonObject.getLong("end");
        }
    }
}
