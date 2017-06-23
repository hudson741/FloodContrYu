package com.floodCtr.storm;

import com.floodCtr.generate.StormThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/20
 */
public class Client {

    public static void main(String[] args)
            throws TException
    {
        // 传输层
        TTransport transport = new TSocket("119.29.65.85", 9000);
        System.out.println("1");
        transport.open();
        // 协议层
        TProtocol protocol = new TBinaryProtocol(transport);
        System.out.println("2");
        // 创建RPC客户端
        StormThriftService.Client client = new StormThriftService.Client(protocol);
        System.out.println("3");
        // 调用服务
        String arg="storm nimbus -c storm.zookeeper.servers=[\"10.186.58.13\"] -c nimbus.seeds=[\"192.168.10.3\",\"192.168.10.4\"] -c nimbus.thrift.port=9005 -c ui.port=9002";

        client.addSupervisor("ui-fuck","192.168.10.5",arg,new HashMap<String, String>());
        System.out.println("4");
        // 关闭通道
        transport.close();
        System.out.println("5");
    }
}
