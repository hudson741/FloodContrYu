package com.yss.yarn.resource;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.storm.shade.org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.yss.yarn.StormDockerService;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/8
 */
public class IPManager {
    private static final Logger   LOG                   = LoggerFactory.getLogger(IPManager.class);
    private static IPManager      ipManager             = new IPManager();
    private final String          BASE_PATH             = "/docker/storm";
    private final String          PATH_NIMBUS           = "/docker/storm/nimbus";
    private final String          PATH_UI               = "/docker/storm/ui";
    private final String          PATH_SUPERVISOR       = "/docker/storm/supervisor";
    private final String          PATH_HOSTS_NIMBUS     = "/docker/storm/hosts/nimbus";
    private final String          PATH_HOSTS_SUPERVISOR = "/docker/storm/hosts/supervisor";
    private final String          PATH_HOSTS_UI         = "/docker/storm/hosts/ui";
    private CuratorFramework      client                = null;
    private List<String>          nimbus                = Lists.newArrayList();
    private BlockingQueue<String> nimbusIp              = new LinkedBlockingQueue<>();
    private List<String> uiIp                  = Lists.newArrayList();
    private BlockingQueue<String> supervisorIp          = new LinkedBlockingQueue<>();

    private IPManager() {
        client = CuratorFrameworkFactory.newClient("123.207.8.134:2181", new ExponentialBackoffRetry(1000, 3));

        try {
            client.start();
            clear(PATH_NIMBUS);
            clear(PATH_UI);
            clear(PATH_SUPERVISOR);
            clear(PATH_HOSTS_NIMBUS);
            clear(PATH_HOSTS_SUPERVISOR);
            clear(PATH_HOSTS_UI);
        } catch (Exception e) {
            e.printStackTrace();
        }

        nimbus.add("192.168.10.3");
        nimbus.add("192.168.10.4");
        nimbusIp.add("192.168.10.3");
        nimbusIp.add("192.168.10.4");
        uiIp.add("192.168.10.5");

        for (int i = 6; i < 23; i++) {
            supervisorIp.add("192.168.10." + i);
        }
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

    public void relaseNimbus(String ip) {
        nimbusIp.add(ip);
    }

    public void relaseSupervisorIp(String ip) {
        supervisorIp.add(ip);
    }

    public void relaseUIIp(String ip) {
        uiIp.add(ip);
    }

    public void remove(StormDockerService.PROCESS process, String path) {
        String cpath = getCpath(process, path);

        try {
            client.delete().forPath(cpath);
            client.sync();
        } catch (Exception e) {
            LOG.info("fuck IPMANAGER remove", e);
        }
    }

    private String getCpath(StormDockerService.PROCESS process, String path) {
        if (process == StormDockerService.PROCESS.NIMBUS) {
            return PATH_NIMBUS + "/" + path;
        } else if (process == StormDockerService.PROCESS.UI) {
            return PATH_UI + "/" + path;
        } else {
            return PATH_SUPERVISOR + "/" + path;
        }
    }

    public void setHosts2Zk(StormDockerService.PROCESS process, String child, String value) {
        String hpath = getHpath(process, child);
        byte[] bytes = value.getBytes();

        try {
            if (client.checkExists().forPath(hpath) != null) {
                client.setData().forPath(hpath, bytes);
            } else {
                client.create().creatingParentsIfNeeded().forPath(hpath, bytes);
            }
        } catch (Exception e) {
            LOG.info("fuck IPMANAGER setIp2Zk", e);
        }
    }

    private String getHpath(StormDockerService.PROCESS process, String path) {
        if (process == StormDockerService.PROCESS.NIMBUS) {
            return PATH_HOSTS_NIMBUS + "/" + path;
        } else if (process == StormDockerService.PROCESS.UI) {
            return PATH_HOSTS_UI + "/" + path;
        } else {
            return PATH_HOSTS_SUPERVISOR + "/" + path;
        }
    }

    public String getIPByProcess(StormDockerService.PROCESS process) {
        if (process.equals(StormDockerService.PROCESS.NIMBUS)) {
            return this.getNimbusIp();
        } else if (process.equals(StormDockerService.PROCESS.UI)) {
            return this.getUIIp();
        } else if (process.equals(StormDockerService.PROCESS.SUPERVISOR)) {
            return this.getSupervisorIp();
        }

        return "192.168.0.24";
    }

    public static IPManager getInstance() {
        return ipManager;
    }

    public void setIp2Zk(StormDockerService.PROCESS process, String child, String value) {
        String cpath = getCpath(process, child);
        byte[] bytes = value.getBytes();

        try {
            if (client.checkExists().forPath(cpath) != null) {
                client.setData().forPath(cpath, bytes);
            } else {
                client.create().creatingParentsIfNeeded().forPath(cpath, bytes);
            }
        } catch (Exception e) {
            LOG.info("fuck IPMANAGER setIp2Zk", e);
        }
    }

    public String getNimbusIp() {
        return nimbusIp.poll();
    }

    public List<String> getNimbusSeeds() {
        return nimbus;
    }

    public String getSupervisorIp() {
        return supervisorIp.poll();
    }

    public String getUIIp() {
        return uiIp.get(0);
    }
}
