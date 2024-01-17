package com.learnFlink.dataSourcesTestDrive;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Synchronized;
import lombok.ToString;

import java.math.BigDecimal;


@ToString
@NoArgsConstructor
@AllArgsConstructor
public class LogPOJO {
    public String activity;
    public BigDecimal unified_time;
    public String process_global_uid;
    public String agent_id;

}

