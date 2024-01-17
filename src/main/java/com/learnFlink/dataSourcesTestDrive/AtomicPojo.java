package com.learnFlink.dataSourcesTestDrive;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;


@ToString
@NoArgsConstructor
@AllArgsConstructor
public class AtomicPojo {
    AtomicInteger i;
}
