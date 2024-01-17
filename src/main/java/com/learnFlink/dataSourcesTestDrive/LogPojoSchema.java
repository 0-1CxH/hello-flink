package com.learnFlink.dataSourcesTestDrive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

class LogPojoSchema implements DeserializationSchema<LogPOJO> {
    @Override
    public LogPOJO deserialize(byte[] bytes) throws IOException {
        String str = new String(bytes);
        JSONObject json_obj = JSON.parseObject(str);

        LogPOJO log_pojo = new LogPOJO(
                json_obj.getString("activity"),
                json_obj.getBigDecimal("unified_time"),
                json_obj.getString("pguid"),
                json_obj.getString("agent_id")
        );
        return log_pojo;
    }

    @Override
    public boolean isEndOfStream(LogPOJO logPOJO) {
        return false;
    }

    @Override
    public TypeInformation<LogPOJO> getProducedType() {
        return TypeInformation.of(LogPOJO.class);
    }
}
