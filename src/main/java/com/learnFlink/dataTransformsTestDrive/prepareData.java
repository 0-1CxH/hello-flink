package com.learnFlink.dataTransformsTestDrive;

import com.learnFlink.dataSourcesTestDrive.LogPOJO;
import lombok.extern.java.Log;

import java.math.BigDecimal;
import java.util.ArrayList;

public class prepareData {
    LogPOJO elem_1 = new LogPOJO(
            "process_create",
            new BigDecimal("1645327532920"),
            "660fa076-cb11-41e7-b855-c6b3f8b0cd3c",
            "6152873b378feae2");
    LogPOJO elem_2 = new LogPOJO(
            "process_exit",
            new BigDecimal("1645327532921"),
            "266a608b-ad53-4b64-a3b9-ca1889c8cac3",
            "6152873b378feae2"
    );

    public ArrayList<LogPOJO> get(){
        ArrayList<LogPOJO> dataCollection = new ArrayList<>();
        dataCollection.add(elem_1);
        dataCollection.add(elem_2);
        dataCollection.add(elem_1);
        dataCollection.add(elem_2);
        dataCollection.add(elem_1);
        dataCollection.add(elem_2);
        return dataCollection;
    }




}
