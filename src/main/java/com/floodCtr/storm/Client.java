package com.floodCtr.storm;

import com.alibaba.fastjson.JSONObject;
import com.floodCtr.generate.FloodContrThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;
import java.util.List;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/20
 */
public class Client {

    public static void main(String[] args)
            throws TException, InterruptedException {
        // 传输层
//        TTransport transport = new TSocket("119.29.65.85", 9000);
        TTransport transport = new TSocket("zhangc5", 9000);
        System.out.println("1");
        transport.open();
        // 协议层
        TProtocol protocol = new TBinaryProtocol(transport);
        System.out.println("2");
        // 创建RPC客户端
        FloodContrThriftService.Client client = new FloodContrThriftService.Client(protocol);
        System.out.println("3");

        System.out.println(client.getAllDockerJob());

        String json = client.getStormNimbus();

        List<String> list = JSONObject.parseArray(json,String.class);

        for(String nimbus:list){
            String[] fuck = nimbus.split(":");
            for(String d:fuck){
                System.out.println(d);
            }
        }

        System.out.println(client.getStormUi());

        // 调用服务
        System.out.println("4");
        // 关闭通道
        transport.close();
        System.out.println("5");
    }
}
