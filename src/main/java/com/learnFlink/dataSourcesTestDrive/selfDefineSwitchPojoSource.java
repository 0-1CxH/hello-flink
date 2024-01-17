package com.learnFlink.dataSourcesTestDrive;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.math.BigDecimal;

import static java.lang.Thread.sleep;

public class selfDefineSwitchPojoSource implements ParallelSourceFunction<SwitchInfoPojo> {
    protected boolean is_running = true;
    protected static int elemCount = 0;
    protected static boolean lastStatus = false;

    synchronized protected SwitchInfoPojo getNewRandomInstance(){
        SwitchInfoPojo newInstance = new SwitchInfoPojo(
                elemCount,
                !lastStatus
        );
        elemCount++;
        lastStatus = !lastStatus;
        return newInstance;
    }

    @Override
    public void run(SourceContext<SwitchInfoPojo> sourceContext) throws Exception {
        while(is_running){
            sourceContext.collect(getNewRandomInstance());
            sleep(1500);
        }
    }

    @Override
    public void cancel() {
        is_running = false;
    }
}
