package com.learnFlink.dataSourcesTestDrive;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AllowListInfoPojo {
    public long id;
    public boolean op;
    public long timestamp;
}
