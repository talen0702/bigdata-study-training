package com.myrpc.impl;

import com.myrpc.MyRpcInterface;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

@Slf4j
public class MyRpcInterfaceImpl implements MyRpcInterface {
    @Override
    public String findName(String studentId) {
        if (null == studentId) return "";
        String msg = "";
        switch (studentId){
            case "20210000000000":
                msg = null;
                break;
            case "20210123456789":
                msg = "心心";
                break;
            case "G20190343020110":
                msg = "黄雄";
                break;
        }
        return msg;
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return MyRpcInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
