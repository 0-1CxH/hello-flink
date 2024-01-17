package com.learnFlink.dataSourcesTestDrive;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

public class selfDefineLogPojoSource implements ParallelSourceFunction<LogPOJO> {
    protected boolean is_running = true;
    protected static int elemCount = 0;

    synchronized protected LogPOJO getNewRandomInstance(){
        LogPOJO newInstance = new LogPOJO(
                String.valueOf(Math.random()),
                BigDecimal.valueOf(elemCount),
                String.valueOf(Math.random()),
                String.valueOf(Math.random())
        );
        elemCount++;
        return newInstance;
    }

    @Override
    public void run(SourceContext<LogPOJO> sourceContext) throws Exception {
        while(is_running){
            sourceContext.collect(getNewRandomInstance());
            sleep(300);
        }
    }

    @Override
    public void cancel() {
        is_running = false;
    }
}
