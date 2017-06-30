package com.floodCtr.rpc;

import com.floodCtr.FloodContrHeartBeat;
import com.floodCtr.FloodContrSubScheduler;
import com.floodCtr.YarnClient;
import com.floodCtr.monitor.FloodContrRunningMonitor;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public abstract class FloodContrMaster extends ThriftServer{

    private static final Logger LOG = LoggerFactory.getLogger(FloodContrMaster.class);

    private final String FLOOD_MASTER_SERVER = "/floodContrYu/masterServer";

    private YarnClient yarnClient =  YarnClient.getInstance();

    private CuratorFramework client                = null;

    public FloodContrMaster(TProcessor processor,int port) {
        super(processor,port);
    }

    public abstract void initExecute();

    public void clear(String path) {
        try {
            List<String> list = client.getChildren().forPath(path);

            if (CollectionUtils.isNotEmpty(list)) {
                for (String node : list) {
                    client.delete().forPath(path + "/" + node);
                }
            }

            client.delete().forPath(path);
            client.sync();
        } catch (Exception e) {
            LOG.error("fuck ", e);
        } finally {
            try {
                client.delete().forPath(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void synchronizeServerAdress2ZK(){
        try {
            clear(FLOOD_MASTER_SERVER);
            String address =   InetAddress.getLocalHost().getHostAddress()+":9000";
            byte[] bytes = address.getBytes();
            if (client.checkExists().forPath(FLOOD_MASTER_SERVER) != null) {
                client.setData().forPath(FLOOD_MASTER_SERVER, bytes);
            } else {
                client.create().creatingParentsIfNeeded().forPath(FLOOD_MASTER_SERVER, bytes);
            }
        } catch (Exception e) {
            LOG.info("fuck IPMANAGER setIp2Zk", e);
        }
        client.close();

    }

    private void initCurator(){
        String       zk         = System.getenv("zk");
        List<String> zkList     = Lists.newArrayList();
        String[]     zkArray    = zk.split(",");

        for (String zkHost : zkArray) {
            zkList.add(zkHost);
        }
        client = CuratorFrameworkFactory.newClient(zkList.get(0)+":2181", new ExponentialBackoffRetry(1000, 3));

        client.start();
    }

    public void start() throws IOException, YarnException {
        try {

            initCurator();

            synchronizeServerAdress2ZK();

            // 注册
            registerAppMaster(yarnClient,port);


            FloodContrHeartBeat floodContrHeartBeat = new FloodContrHeartBeat(yarnClient);

            FloodContrSubScheduler floodContrSubScheduler = new FloodContrSubScheduler(yarnClient);

            FloodContrRunningMonitor floodContrHealthCheck = new FloodContrRunningMonitor(yarnClient);


            initExecute();

            floodContrHeartBeat.scheduleHeartBeat(1, TimeUnit.SECONDS);

            floodContrSubScheduler.scheduleSubJob(1,TimeUnit.SECONDS);

            floodContrHealthCheck.monitor();


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
