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
