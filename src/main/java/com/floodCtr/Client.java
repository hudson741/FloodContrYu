package com.floodCtr;

import com.floodCtr.storm.StormService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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
        TTransport transport = new TSocket("127.0.0.1", 9000);
        transport.open();
        // 协议层
        TProtocol protocol = new TBinaryProtocol(transport);

        // 创建RPC客户端
        StormService.Client client = new StormService.Client(protocol);

        // 调用服务
        String result = client.addSupervisor("2");
        System.out.printf("-->" + result);

        // 关闭通道
        transport.close();
    }
}
