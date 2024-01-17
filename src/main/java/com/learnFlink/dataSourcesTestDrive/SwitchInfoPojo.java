package com.learnFlink.dataSourcesTestDrive;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SwitchInfoPojo {
    public long timestamp;
    public boolean isOpen;
}
