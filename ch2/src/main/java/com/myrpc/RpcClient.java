package com.myrpc;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.conf.Configuration;
import java.net.InetSocketAddress;

public class RpcClient {
    private static final InetSocketAddress inetSocketAddress = new InetSocketAddress(
            "127.0.0.1", 9080);
    public static void main(String[] args) {
        System.out.println(args[0]);
        try{
            MyRpcInterface proxy = RPC.getProxy(MyRpcInterface.class,1L,inetSocketAddress,new Configuration());
            System.out.println(proxy.findName(args[0]));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
