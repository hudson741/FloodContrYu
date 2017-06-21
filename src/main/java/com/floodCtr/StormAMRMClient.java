/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */



package com.yss.yarn;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yss.yarn.model.StormDC;
import com.yss.yarn.resource.IPManager;
import com.yss.yarn.resource.StormDCManager;

public class StormAMRMClient extends AMRMClientImpl<ContainerRequest> {
    private static final Logger     LOG            = LoggerFactory.getLogger(StormAMRMClient.class);
    private IPManager               ipManager      = IPManager.getInstance();
    private StormDCManager          stormDCManager = StormDCManager.getInstance();
    private Resource                maxResourceCapability;
    @SuppressWarnings("rawtypes")
    private final Map               storm_conf;
    private final YarnConfiguration hadoopConf;
    private final Set<Container>    containers;
    private NMClientImpl            nmClient;

    public StormAMRMClient(@SuppressWarnings("rawtypes") Map storm_conf, YarnConfiguration hadoopConf) {
        this.storm_conf = storm_conf;
        this.hadoopConf = hadoopConf;
        this.containers = new TreeSet<Container>();

        // start am nm client
        nmClient = (NMClientImpl) NMClient.createNMClient();
        nmClient.init(hadoopConf);
        nmClient.start();
    }

    /**
     * 设置启动storm的docker容器的命令
     * @param process
     * @param containerName
     * @param zkHosts
     * @param nimbusSeeds
     * @param nimbusThriftPort
     * @param memory
     * @param cpu
     * @param uiPort
     * @param ip
     * @return
     */
    public List<String> buildStormOnDockerCommond(StormDockerService.PROCESS process, String containerName,
                                                  List<Object> zkHosts, List<String> nimbusSeeds, List<Object> hostIps,
                                                  int nimbusThriftPort, int memory, int cpu, int uiPort, String ip,
                                                  String hostName) {
        Vector<CharSequence> vargs = new Vector<CharSequence>(100);

        vargs.add("sh runStormOnDocker.sh ");

        // 1 containerName
        vargs.add("'" + containerName);

        // 4 nimbus port
        // 如果同一台服务器启动了两台nimbus docker，应该采用不同端口，或一个不绑定到本地，避免冲突
        vargs.add(String.valueOf(nimbusThriftPort));

        // 5  memery
        vargs.add(memory + "MB");

        // 6  cpu
        vargs.add(String.valueOf(cpu));

        // 7  uiport
        vargs.add(String.valueOf(uiPort));

        // 8  process
        vargs.add(process.getCode());

        // 9 ip
        vargs.add(ip);
        vargs.add(hostName + "'");

        // zk
        String zkHostsArrays = "'";

        for (Object zkHost : zkHosts) {
            zkHostsArrays += zkHost + " ";
        }

        zkHostsArrays = zkHostsArrays.substring(0, zkHostsArrays.length() - 1) + "'";
        vargs.add(zkHostsArrays);
        //zk done


        // nimbus.seeds
        String nmSeedsArrays = "'";

        for (String nimbus : nimbusSeeds) {
            nmSeedsArrays += nimbus + " ";
        }

        nmSeedsArrays = nmSeedsArrays.substring(0, nmSeedsArrays.length() - 1) + "'";
        vargs.add(nmSeedsArrays);
        //nimbus.seeds done


        //hostIps
        if(hostIps!=null && hostIps.size()>0) {
            String hostIpsArrays = "'";
            for (Object hostIp:hostIps){
                hostIpsArrays += hostIp+" ";
            }
            hostIpsArrays = hostIpsArrays.substring(0, hostIpsArrays.length() - 1) + "'";
            vargs.add(hostIpsArrays);
        }


        vargs.add("1> /opt/stdout");
        vargs.add("2> /opt/stderr");

        StringBuilder command = new StringBuilder();

        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<String>();

        commands.add(command.toString());

        return commands;
    }

    public void launchStormComponentOnDocker(Container container, StormDockerService.PROCESS process, String dockerIp)
            throws IOException {
        Map<String, String>        env            = new HashMap<String, String>();
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        String                     shellPath      = (String) storm_conf.get("runStormOnDocker.path");
        Path                       sp             = new Path(shellPath);
        FileSystem                 fs             = FileSystem.get(hadoopConf);

        localResources.put("runStormOnDocker.sh",
                           Util.newYarnAppResource(fs, sp, LocalResourceType.FILE, LocalResourceVisibility.PUBLIC));

        ContainerId  containerId       = container.getId();
        int          memory            = container.getResource().getMemory();
        int          cpu               = container.getResource().getVirtualCores();
        List<Object> zkHostsArrayConf  = (List) storm_conf.get("storm.zookeeper.servers");
        List<String> nmSeedsArraysConf = IPManager.getInstance().getNimbusSeeds();
        int          nmtPort           = (Integer) storm_conf.get("nimbus.thrift.port");
        int          uiPort            = (storm_conf.get("ui.port") != null)
                                         ? (Integer) storm_conf.get("ui.port")
                                         : 9092;

        LOG.info("fuck with storm_conf ui:"+uiPort +" nimbus "+nmtPort);
        // 连接kafka，或者其他服务所用到的host：ip 配置信息
        List<Object> hostIpArrayConf = null;

        Object hostIpObj = storm_conf.get("host.ip.matchlist");

        if(hostIpObj !=null){
            hostIpArrayConf = (List)hostIpObj;
        }

        String       dockerHostName  = process.getCode() + "-" + container.getId().getContainerId();
        List<String> commands        = buildStormOnDockerCommond(process,
                                                                 dockerHostName,
                                                                 zkHostsArrayConf,
                                                                 nmSeedsArraysConf,
                                                                 hostIpArrayConf,
                                                                 nmtPort,
                                                                 memory,
                                                                 cpu,
                                                                 uiPort,
                                                                 dockerIp,
                                                                 dockerHostName);

        LOG.info("fuck 33  " + commands.toString());

        ContainerLaunchContext launchContext = ContainerLaunchContext.newInstance(localResources,
                                                                                  env,
                                                                                  commands,
                                                                                  null,
                                                                                  null,
                                                                                  null);

        try {
            nmClient.startContainer(container, launchContext);

            String ip     = container.getNodeHttpAddress().split(":")[0];
            NodeId nodeId = container.getNodeId();

            StormDCManager.list.add(new StormDC(containerId, nodeId, process));
            LOG.info("fuck add stormContainer  " + containerId + "  " + nodeId);
            LOG.info("fuck container env " + ip + "  " + nodeId);

            if (process == StormDockerService.PROCESS.NIMBUS) {

                /**
                 *                            {
                 *                               zhangc1-->9000
                 * zk---> /docker/storm/nimbus   zhangc3-->9000
                 *
                 *                            }
                 */
                ipManager.setIp2Zk(process,
                                   ip + ":" + dockerHostName,
                                   String.valueOf(storm_conf.get("nimbus.thrift.port")));
            } else if (process == StormDockerService.PROCESS.UI) {
                ipManager.setIp2Zk(process, ip + ":" + dockerHostName, String.valueOf(storm_conf.get("ui.port")));
            } else {
                ipManager.setIp2Zk(process, ip + ":" + dockerHostName, dockerIp);
            }
        } catch (Exception e) {
            LOG.error("Caught an exception while trying to start a container", e);
            System.exit(-1);
        }
    }

    private synchronized void releaseAllSupervisorsRequest() {
        Iterator<Container> it = this.containers.iterator();
        ContainerId         id;

        while (it.hasNext()) {
            id = it.next().getId();
            LOG.debug("Releasing container (id:" + id + ")");
            releaseAssignedContainer(id);
            it.remove();
        }
    }

    public void stormDockerContainerHealthCheck() {
        stormDCManager.circleStormContainerStatusCheck(nmClient, this);
    }

    public void setMaxResource(Resource maximumResourceCapability) {
        this.maxResourceCapability = maximumResourceCapability;
        LOG.info("Max Capability is now " + this.maxResourceCapability);
    }

    public Map getStorm_conf() {
        return storm_conf;
    }
}
