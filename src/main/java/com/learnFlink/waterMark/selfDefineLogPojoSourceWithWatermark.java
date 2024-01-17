package com.learnFlink.waterMark;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import com.learnFlink.dataSourcesTestDrive.selfDefineLogPojoSource;
import org.apache.flink.streaming.api.watermark.Watermark;

import static java.lang.Thread.sleep;

public class selfDefineLogPojoSourceWithWatermark extends selfDefineLogPojoSource {
    @Override
    public void run(SourceContext<LogPOJO> sourceContext) throws Exception {
        while (is_running) {
            LogPOJO newInstance = getNewRandomInstance();
            long timestamp = Long.parseLong(String.valueOf(newInstance.unified_time));
            sourceContext.collectWithTimestamp(newInstance, timestamp);
            sourceContext.emitWatermark(new Watermark(timestamp - 1));
            sleep(3000);
        }
    }
}
