package com.floodCtr.storm;

import java.io.IOException;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floodCtr.YarnClient;
import com.floodCtr.job.FloodJob;
import com.floodCtr.publish.FloodContrJobPubProxy;
import com.floodCtr.rpc.FloodContrMaster;

import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class MasterServer {
    private static final Logger logger = LoggerFactory.getLogger(StormThriftServiceImpl.class);

    public static void addStormComponent(FloodContrJobPubProxy floodContrJobPubProxy, String imageName, String node,
                                         String businessTag, String containerName, String dockerIp, String dockerArgs,
                                         Map<String, String> port, List<String> zkList, FloodJob.CM cm, String appId,
                                         List<String> nimbusSeedsList, List<String> drpcList,
                                         FloodJob.PRIORITY priority)
            throws TException {
        String   jobId      = UUID.randomUUID().toString();
        String   hadoopUser = System.getenv("hadoopUser");
        FloodJob floodJob   = new FloodJob(jobId, cm);
        String   netUrl     = System.getenv("netUrl");

        // zk服务
        if (CollectionUtils.isNotEmpty(zkList)) {
            String zkHostsArrays = "[";

            for (Object zkHost : zkList) {
                zkHostsArrays = zkHostsArrays + "\\\"" + zkHost + "\\\" ";
            }

            zkHostsArrays = zkHostsArrays.substring(0, zkHostsArrays.length() - 1) + "]";
            dockerArgs    = dockerArgs + " -c storm.zookeeper.servers=" + zkHostsArrays;
        }

        // nimbus 配置
        if (CollectionUtils.isNotEmpty(nimbusSeedsList)) {
            String nimbusSeedsArray = "[";

            for (Object nimbus : nimbusSeedsList) {
                nimbusSeedsArray = nimbusSeedsArray + "\\\"" + nimbus + "\\\"" + ",";
            }

            nimbusSeedsArray = nimbusSeedsArray.substring(0, nimbusSeedsArray.length() - 1) + "]";
            dockerArgs       = dockerArgs + " -c nimbus.seeds=" + nimbusSeedsArray;
        }

        // drpc配置
        if (CollectionUtils.isNotEmpty(drpcList)) {
            String drpcArray = "[";

            for (Object drpc : drpcList) {
                drpcArray = drpcArray + "\\\"" + drpc + "\\\"" + ",";
            }

            drpcArray  = drpcArray.substring(0, drpcArray.length() - 1) + "]";
            dockerArgs = dockerArgs + " -c drpc.servers=" + drpcArray;
        }

        floodJob.netUrl(netUrl);

        if (StringUtils.isNotEmpty(node)) {
            floodJob.nodeBind(node);
        }

        logger.info("fuck dockerargs " + dockerArgs);
        floodJob.businessTag(businessTag).priority(priority);

        String localDir = StringUtils.isEmpty(appId)
                          ? "/home/" + hadoopUser + "/stormlog"
                          : "/home/" + hadoopUser + "/stormlog/" + appId;

        floodJob.buildDockerCMD()
                .imageName(imageName)
                .containerName(containerName)
                .hostName(containerName)
                .host(containerName, dockerIp)
                .ip(dockerIp)
                .ports(port)
                .volume(localDir, "/opt/storm/logs")
                .volume("/etc/localtime", "/etc/localtime")
                .dockerArgs(dockerArgs);
        floodContrJobPubProxy.publishJob(floodJob, node, priority);
    }

    public static void main(String[] args) throws IOException, YarnException {

        /**
         * 个性化thrift接口实现，每个使用者自定义实现
         */
        final FloodContrJobPubProxy floodContrJobPubProxy = new FloodContrJobPubProxy(YarnClient.getInstance());
        final FloodContrMaster      floodContrMaster      =
            new FloodContrMaster(new StormThriftService.Processor<>(new StormThriftServiceImpl(YarnClient.getInstance(),
                                                                                               floodContrJobPubProxy)),
                                 9050) {
            @Override
            public void initExecute() {
                String       dockerUiIp          = System.getenv("uiIp");
                String       zk                  = System.getenv("zk");
                String       nimbusSeeds         = System.getenv("nimbusSeeds");
                String       drpc                = System.getenv("drpc");
                String       nimbusUIDockerImage = System.getenv("nimbusUIDockerImage");
                String       appId               = System.getenv("appId");
                int       nimbusPort             = StringUtils.isEmpty(System.getenv("nimbusPort"))?9005:Integer.parseInt(System.getenv("nimbusPort"));
                int       uiPort                 = StringUtils.isEmpty(System.getenv("uiPort"))?9092:Integer.parseInt(System.getenv("uiPort"));
                int       drpcPort               = StringUtils.isEmpty(System.getenv("drpcPort"))?3772:Integer.parseInt(System.getenv("drpcPort"));
                List<String> zkList              = Lists.newArrayList();
                String[]     zkArray             = zk.split(",");

                for (String zkHost : zkArray) {
                    zkList.add(zkHost);
                }

                // 当前所有可用物理节点
                List<String> nodeList = floodContrJobPubProxy.yarnClient.getNodes();
                Object[]     nodes    = nodeList.toArray();

                for (Object host : nodes) {
                    logger.info("fuck host " + host + "");
                }

                List<String> nimbusSeedsList = Lists.newArrayList();
                String[]     nimbusArray     = nimbusSeeds.split(",");

                for (String nimbusHost : nimbusArray) {
                    nimbusSeedsList.add(nimbusHost);
                }

                List<String> drpcServersList = Lists.newArrayList();

                if (StringUtils.isNotEmpty(drpc)) {
                    String[] drpcArray = drpc.split(",");

                    for (String drpcHost : drpcArray) {
                        drpcServersList.add(drpcHost);
                    }
                }

                /**
                 * 启动drpc节点
                 */
                String drpcDockerArgs = "storm drpc -c drpc.port="+drpcPort;

                for (int i = drpcServersList.size() - 1; i >= 0; i--) {
                    try {
                        Map<String, String> port = new HashMap<>();

                        port.put(drpcPort+"", drpcPort+"");
                        MasterServer.addStormComponent(floodContrJobPubProxy,
                                                       nimbusUIDockerImage,
                                                       nodes[i] + "",
                                                       "drpc",
                                                       "drpc-" + System.currentTimeMillis(),
                                                       drpcServersList.get(i),
                                                       drpcDockerArgs,
                                                       port,
                                                       zkList,
                                                       FloodJob.CM.CMLOW,
                                                       appId,
                                                       nimbusSeedsList,
                                                       drpcServersList,
                                                       FloodJob.PRIORITY.HIGH);
                    } catch (Exception e) {
                        logger.error("error ", e);
                    }
                }

                /**
                 * 启动 numbus节点
                 */
                String nimbusDockerArgs = "storm  nimbus  -c nimbus.thrift.port="+nimbusPort;

                for (int i = 0; i < nimbusSeedsList.size(); i++) {
                    try {
                        Map<String, String> port = new HashMap<>();

                        port.put(nimbusPort+"", nimbusPort+"");
                        MasterServer.addStormComponent(floodContrJobPubProxy,
                                                       nimbusUIDockerImage,
                                                       nodes[i] + "",
                                                       "nimbus",
                                                       "nimbus-" + System.currentTimeMillis(),
                                                       nimbusSeedsList.get(i),
                                                       nimbusDockerArgs,
                                                       port,
                                                       zkList,
                                                       FloodJob.CM.CMLOW,
                                                       appId,
                                                       nimbusSeedsList,
                                                       drpcServersList,
                                                       FloodJob.PRIORITY.HIGH);
                    } catch (TException e) {
                        logger.error("error ", e);
                    }
                }

                /**
                 * 启动ui节点
                 */
                String              uiDockerArgs = "storm ui -c ui.port="+uiPort+" -c nimbus.thrift.port="+nimbusPort;
                Map<String, String> port         = new HashMap<>();

                port.put(uiPort+"", uiPort+"");

                try {
                    MasterServer.addStormComponent(floodContrJobPubProxy,
                                                   nimbusUIDockerImage,
                                                   null,
                                                   "ui",
                                                   "ui-" + System.currentTimeMillis(),
                                                   dockerUiIp,
                                                   uiDockerArgs,
                                                   port,
                                                   zkList,
                                                   FloodJob.CM.CMLOW,
                                                   appId,
                                                   nimbusSeedsList,
                                                   drpcServersList,
                                                   FloodJob.PRIORITY.LOW);
                } catch (TException e) {
                    logger.error("error ", e);
                }
            }
        };

        floodContrMaster.start();
    }
}
