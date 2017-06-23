package com.floodCtr.rpc;

import com.floodCtr.FloodContrHeartBeat;
import com.floodCtr.FloodContrSubScheduler;
import com.floodCtr.YarnClient;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public abstract class FloodContrMaster extends ThriftServer{

    private static final Logger LOG = LoggerFactory.getLogger(FloodContrMaster.class);
    private YarnClient yarnClient =  YarnClient.getInstance();

    public FloodContrMaster(TProcessor processor,int port) {
        super(processor,port);
    }

    public abstract void initExecute();

    public void start() throws IOException, YarnException {
        try {

            // 注册
            registerAppMaster(yarnClient,port);

            FloodContrHeartBeat floodContrHeartBeat = new FloodContrHeartBeat(yarnClient);

            FloodContrSubScheduler floodContrSubScheduler = new FloodContrSubScheduler(yarnClient);


            initExecute();

            floodContrHeartBeat.scheduleHeartBeat(1, TimeUnit.SECONDS);

            floodContrSubScheduler.scheduleSubJob(1,TimeUnit.SECONDS);


            LOG.info("server begin to start");
            System.out.println("server begin to start");
            serve();
        } catch (TTransportException e) {
           LOG.error("error ",e);
        }

    }

    public RegisterApplicationMasterResponse registerAppMaster(YarnClient yarnClient,int port)
            throws IOException, YarnException {
        final String                      host   = InetAddress.getLocalHost().getHostName();
        final String                      target = host + ":" + port;
        InetSocketAddress addr   = NetUtils.createSocketAddr(target);
        LOG.info("registerAppMaster  :" + target+"  hostName :"+ addr.getHostName());
        RegisterApplicationMasterResponse resp   = yarnClient.registerApplicationMaster(addr.getHostName(), port, null);
        LOG.info("Got a registration response " + resp);
        LOG.info("Max Capability " + resp.getMaximumResourceCapability());

        return resp;
    }

}
