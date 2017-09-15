package com.floodCtr.rpc;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private TServerSocket       serverTransport;
    private TServer             server;
    protected TProcessor        processor;
    protected int               port;

    public ThriftServer(TProcessor processor, int port) {
        this.processor = processor;
        this.port      = port;
    }

    protected void serve() throws TTransportException {
        serverTransport = new TServerSocket(port);

        TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory(true, true);

        server = new TThreadPoolServer(
            new TThreadPoolServer.Args(serverTransport).protocolFactory(protocolFactory).processor(processor));
        server.serve();
    }
}
