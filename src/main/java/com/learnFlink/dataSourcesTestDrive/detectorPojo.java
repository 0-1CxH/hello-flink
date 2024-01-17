package com.learnFlink.dataSourcesTestDrive;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class detectorPojo {
    public String detectorID;
    public boolean alarm;
    public int value;
    public long timestamp;
}
