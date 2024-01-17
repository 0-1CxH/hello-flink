package com.learnFlink.dataSourcesTestDrive;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OrderInfoPojo {
    public long orderID;
    public double payment;
    public boolean done;
    public String source;
    public long timestamp;
}
