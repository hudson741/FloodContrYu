package com.floodCtr.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.floodCtr.FloodContrHeartBeat;
import com.floodCtr.FloodContrSubScheduler;
import com.floodCtr.YarnClient;
import com.floodCtr.monitor.FloodContrRunningMonitor;
import com.floodCtr.monitor.FloodJobRunningState;

import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public abstract class FloodContrMaster extends ThriftServer {
    private static final Logger LOG                 = LoggerFactory.getLogger(FloodContrMaster.class);
    private final String        FLOOD_MASTER_SERVER = "/floodContrYu/masterServer";
    private YarnClient          yarnClient          = YarnClient.getInstance();
    private CuratorFramework    client              = null;

    public FloodContrMaster(TProcessor processor, int port) {
        super(processor, port);
    }

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

    private void initCurator() {
        String       zk      = System.getenv("zk");
        List<String> zkList  = Lists.newArrayList();
        String[]     zkArray = zk.split(",");

        for (String zkHost : zkArray) {
            zkList.add(zkHost);
        }

        String stormZkPort = StringUtils.isEmpty(System.getenv("stormZkPort"))?"2181":System.getenv("stormZkPort");
        client = CuratorFrameworkFactory.newClient(zkList.get(0) + ":"+stormZkPort, new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    public abstract void initExecute();

    public static void main(String[] args) {
        String json =
            "[{\"businessType\":\"storm-1\",\"floodJob\":{\"businessTag\":\"nimbus\",\"cpu\":1,\"dockerCMD\":{\"containerName\":\"nimbus-1505286926022\",\"dockerArgs\":\"storm  nimbus  -c nimbus.thrift.port=9005 -c storm.zookeeper.servers=[\\\\\\\"10.186.58.13\\\\\\\"] -c nimbus.seeds=[\\\\\\\"192.168.10.3\\\\\\\",\\\\\\\"192.168.10.4\\\\\\\"]\",\"host\":{\"nimbus-1505286926022\":\"192.168.10.3\"},\"hostName\":\"nimbus-1505286926022\",\"imageName\":\"1187655234/storm-1.1.0-1c2g:1.0\",\"ip\":\"192.168.10.3\",\"port\":{\"9005\":\"9005\"},\"volume\":{\"/home/hadoop/stormlog\":\"/opt/storm/logs\"}},\"jobId\":\"19d3886c-6b15-4a62-9dc8-cda67d779ee5\",\"launch_type\":\"NEW\",\"memory\":512,\"netUrl\":\"overlay\",\"nodeBind\":\"zhangc4\",\"priority\":\"HIGH\"},\"jobId\":\"19d3886c-6b15-4a62-9dc8-cda67d779ee5\",\"runIp\":\"zhangc4\",\"runningState\":\"RUNNING\"},{\"businessType\":\"storm-1\",\"floodJob\":{\"businessTag\":\"ui\",\"cpu\":1,\"dockerCMD\":{\"containerName\":\"ui-1505286926104\",\"dockerArgs\":\"storm ui -c ui.port=9092 -c nimbus.thrift.port=9005 -c storm.zookeeper.servers=[\\\\\\\"10.186.58.13\\\\\\\"] -c nimbus.seeds=[\\\\\\\"192.168.10.3\\\\\\\",\\\\\\\"192.168.10.4\\\\\\\"]\",\"host\":{\"ui-1505286926104\":\"192.168.10.5\"},\"hostName\":\"ui-1505286926104\",\"imageName\":\"1187655234/storm-1.1.0-1c2g:1.0\",\"ip\":\"192.168.10.5\",\"port\":{\"9092\":\"9092\"},\"volume\":{\"/home/hadoop/stormlog\":\"/opt/storm/logs\"}},\"jobId\":\"1a912160-430a-4267-b88a-f367c7875016\",\"launch_type\":\"NEW\",\"memory\":512,\"netUrl\":\"overlay\",\"priority\":\"LOW\"},\"jobId\":\"1a912160-430a-4267-b88a-f367c7875016\",\"runIp\":\"zhangc5\",\"runningState\":\"RUNNING\"},{\"businessType\":\"storm-1\",\"floodJob\":{\"businessTag\":\"nimbus\",\"cpu\":1,\"dockerCMD\":{\"containerName\":\"nimbus-1505286926103\",\"dockerArgs\":\"storm  nimbus  -c nimbus.thrift.port=9005 -c storm.zookeeper.servers=[\\\\\\\"10.186.58.13\\\\\\\"] -c nimbus.seeds=[\\\\\\\"192.168.10.3\\\\\\\",\\\\\\\"192.168.10.4\\\\\\\"]\",\"host\":{\"nimbus-1505286926103\":\"192.168.10.4\"},\"hostName\":\"nimbus-1505286926103\",\"imageName\":\"1187655234/storm-1.1.0-1c2g:1.0\",\"ip\":\"192.168.10.4\",\"port\":{\"9005\":\"9005\"},\"volume\":{\"/home/hadoop/stormlog\":\"/opt/storm/logs\"}},\"jobId\":\"57096130-f711-44b8-83b8-683c4d3b37c1\",\"launch_type\":\"NEW\",\"memory\":512,\"netUrl\":\"overlay\",\"nodeBind\":\"zhangc3\",\"priority\":\"HIGH\"},\"jobId\":\"57096130-f711-44b8-83b8-683c4d3b37c1\",\"runIp\":\"zhangc3\",\"runningState\":\"RUNNING\"},{\"businessType\":\"storm-1\",\"floodJob\":{\"businessTag\":\"supervisor\",\"cpu\":2,\"dockerCMD\":{\"containerName\":\"supervisor-1505287004413\",\"dockerArgs\":\"storm supervisor -c nimbus.thrift.port=9005 -c ui.port=9092 -c storm.zookeeper.servers=[\\\\\\\"10.186.58.13\\\\\\\"] -c nimbus.seeds=[\\\\\\\"192.168.10.3\\\\\\\",\\\\\\\"192.168.10.4\\\\\\\"]\",\"host\":{\"supervisor-1505287004413\":\"192.168.10.7\"},\"hostName\":\"supervisor-1505287004413\",\"imageName\":\"1187655234/storm-1.1.0-4c4g:1.0\",\"ip\":\"192.168.10.7\",\"port\":{},\"volume\":{\"/home/hadoop/stormlog\":\"/opt/storm/logs\"}},\"jobId\":\"b5ac2e5c-bc3e-4372-af97-e12c71e605e3\",\"launch_type\":\"NEW\",\"memory\":4096,\"netUrl\":\"overlay\",\"priority\":\"HIGH\"},\"jobId\":\"b5ac2e5c-bc3e-4372-af97-e12c71e605e3\",\"runIp\":\"zhangc1\",\"runningState\":\"RUNNING\"}]";
        List<FloodJobRunningState> list = JSONObject.parseObject(json, List.class);

        for (FloodJobRunningState f : list) {
            FloodContrRunningMonitor.floodJobRunningStates.put(f.getJobId(), f);
        }
    }

    public RegisterApplicationMasterResponse registerAppMaster(YarnClient yarnClient, int port)
            throws IOException, YarnException {
        final String      host   = InetAddress.getLocalHost().getHostName();
        final String      target = host + ":" + port;
        InetSocketAddress addr   = NetUtils.createSocketAddr(target);

        LOG.info("registerAppMaster  :" + target + "  hostName :" + addr.getHostName());

        RegisterApplicationMasterResponse resp = yarnClient.registerApplicationMaster(addr.getHostName(), port, null);

        LOG.info("Got a registration response " + resp);
        LOG.info("Max Capability " + resp.getMaximumResourceCapability());

        return resp;
    }

    private boolean reload() {
        String appId = System.getenv("appId");

        LOG.info("reload");

        YarnConfiguration yarnConf = new YarnConfiguration();
        FileSystem        fs;

        try {
            fs = FileSystem.get(yarnConf);

            Path floodStorePath = new Path(fs.getHomeDirectory(),
                                           "store" + Path.SEPARATOR + appId + Path.SEPARATOR + "flood.txt");

            LOG.info("reload  " + fs.getHomeDirectory() + Path.SEPARATOR + "store" + Path.SEPARATOR + appId
                     + Path.SEPARATOR + "flood.txt");

            if (fs.exists(floodStorePath)) {
                FSDataInputStream fsDataInputStream = fs.open(floodStorePath);
                BufferedReader    bufferedReader    = new BufferedReader(new InputStreamReader(fsDataInputStream,
                                                                                               "UTF-8"));
                StringBuilder     jbuilder          = new StringBuilder();
                String            line;

                while ((line = bufferedReader.readLine()) != null) {
                    jbuilder.append(line);
                }

                bufferedReader.close();

                String json = jbuilder.toString();

                if (StringUtils.isEmpty(json)) {
                    return false;
                }

                LOG.info("reload  " + json);

                List<FloodJobRunningState> list = JSONArray.parseArray(json, FloodJobRunningState.class);

                for (FloodJobRunningState f : list) {
                    LOG.info("reload put " + JSONObject.toJSONString(f));
                    FloodContrRunningMonitor.floodJobRunningStates.put(f.getJobId(), f);
                }

                return true;
            }
        } catch (Throwable e) {
            LOG.error("error ", e);
        }

        return false;
    }

    public void start() throws IOException, YarnException {
        try {
            initCurator();
            synchronizeServerAdress2ZK();

            // 注册
            registerAppMaster(yarnClient, port);

            FloodContrHeartBeat      floodContrHeartBeat    = new FloodContrHeartBeat(yarnClient);
            FloodContrSubScheduler   floodContrSubScheduler = new FloodContrSubScheduler(yarnClient);
            FloodContrRunningMonitor floodContrHealthCheck  = new FloodContrRunningMonitor(yarnClient);

            if (!reload()) {
                initExecute();
            }

            floodContrHeartBeat.scheduleHeartBeat(1, TimeUnit.SECONDS);
            floodContrSubScheduler.scheduleSubJob(1, TimeUnit.SECONDS);
            floodContrHealthCheck.monitor();
            LOG.info("server begin to start");
            System.out.println("server begin to start");
            serve();
        } catch (TTransportException e) {
            LOG.error("error ", e);
        }
    }

    private void synchronizeServerAdress2ZK() {
        try {
            clear(FLOOD_MASTER_SERVER);

            String address = InetAddress.getLocalHost().getHostAddress() + ":9050";
            byte[] bytes   = address.getBytes();

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
}
