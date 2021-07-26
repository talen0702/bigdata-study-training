package com.myrpc;

import com.myrpc.impl.MyRpcInterfaceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcServer {
    public static void main(String[] args) {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(9080);
        builder.setProtocol(MyRpcInterface.class);
        builder.setInstance(new MyRpcInterfaceImpl());
        try{
            RPC.Server server = builder.build();
            server.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
