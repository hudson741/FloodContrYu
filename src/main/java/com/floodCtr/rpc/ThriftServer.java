package com.yss.yarn.rpc;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/21
 */
public class ThriftServer {

    private TServerSocket serverTransport;

    private TServer server;

    private TProcessor processor;

    private int port;

    public ThriftServer(TProcessor processor,int port){
        this.processor = processor;
        this.port = port;
    }

    public void serve() throws TTransportException {
        serverTransport = new TServerSocket(port);

        TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory(true, true);

        server = new TThreadPoolServer(
                new TThreadPoolServer.Args(serverTransport).protocolFactory(protocolFactory).processor(
                        processor));
        server.serve();


    }

}
