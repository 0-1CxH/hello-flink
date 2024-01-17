package com.learnFlink.dataSourcesTestDrive;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

public class selfDefineAtomicPojoSource implements ParallelSourceFunction<AtomicPojo> {
    boolean is_running=true;
    static AtomicInteger count = new AtomicInteger(0);

    AtomicPojo getNewRandomInstance(){
        int r = count.incrementAndGet();
        System.out.println(r);
        return new AtomicPojo();
    }


    @Override
    public void run(SourceContext<AtomicPojo> sourceContext) throws Exception {
        while(is_running){
            sourceContext.collect(getNewRandomInstance());
            sleep(3000);
        }
    }

    @Override
    public void cancel() {
        is_running = false;
    }
}
