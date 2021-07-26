package com.myrpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyRpcInterface extends VersionedProtocol {
    long versionID = 1L;
    String findName(String studentId);
}
