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



package com.yss.yarn.rpc;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yss.yarn.*;
import com.yss.yarn.model.DockerResource;
import com.yss.yarn.resource.ContainerLaunchQueue;
import com.yss.yarn.generated.StormService.Processor;
import com.yss.yarn.generated.StormService;

public class MasterServer  extends ThriftServer{
    private static final Logger      LOG = LoggerFactory.getLogger(MasterServer.class);

    public MasterServer(TProcessor processor, int port) {
        super(processor, port);
    }
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        LOG.info("Starting the AM!!!!");

        @SuppressWarnings("rawtypes")
        Map storm_conf = Config.readStormConfig();

        Util.rmNulls(storm_conf);

        StormAMRMClient rmClient = startAppMaster2ResourceManager(storm_conf, ContainerLaunchQueue.launcherQueue);

        // start nimbus
        StormDockerService.circleStormDockerLaunch(rmClient,
                                                   ContainerLaunchQueue.launcherQueue,
                                                   StormDockerService.PROCESS.NIMBUS);

        // start ui
        StormDockerService.circleStormDockerLaunch(rmClient,
                                                   ContainerLaunchQueue.launcherQueue,
                                                   StormDockerService.PROCESS.UI);

        // start supervisor
        StormDockerService.circleStormDockerLaunch(rmClient,
                                                   ContainerLaunchQueue.launcherQueue,
                                                   StormDockerService.PROCESS.SUPERVISOR);

        /**
         * 初始资源申请 nimbus  ui  supervisor*3,改变StormRunningState
         *
         */
        RequestRMClient requestRMClient = RequestRMClient.getInstance();

        requestRMClient.dockerResourceRequest(rmClient, DockerResource.newInstance(1000, 1), StormDockerService.PROCESS.NIMBUS);

        requestRMClient.dockerResourceRequest(rmClient, DockerResource.newInstance(1000, 1), StormDockerService.PROCESS.UI);

        requestRMClient.dockerResourceRequest(rmClient, DockerResource.newInstance(1000, 1), StormDockerService.PROCESS.SUPERVISOR);


        /**
         * 监控container状态
         */

        rmClient.stormDockerContainerHealthCheck();

        StormService.Iface handler = new StormServiceImpl();

        MasterServer server = new MasterServer(new Processor<>(handler),Integer.parseInt(String.valueOf(storm_conf.get(Config.MASTER_THRIFT_PORT))));

        LOG.info("fuck this "+storm_conf.get(Config.MASTER_THRIFT_PORT)+" "+ storm_conf.get("ui.port"));

        // 注册
        RegisterApplicationMasterResponse resp = server.registerAppMaster(rmClient, storm_conf);

        rmClient.setMaxResource(resp.getMaximumResourceCapability());

        try {
            LOG.info("Starting HB thread");
            ContainerLaunchQueue.circleAllocateContainer(rmClient,
                                                         (Integer) storm_conf.get(
                                                             Config.MASTER_HEARTBEAT_INTERVAL_MILLIS));
            server.serve();
            LOG.info("StormAMRMClient::unregisterApplicationMaster");
        } finally {
            LOG.info("Stop RM client");
            rmClient.stop();
        }

        System.exit(0);
    }

    public RegisterApplicationMasterResponse registerAppMaster(StormAMRMClient rmClient, Map storm_conf)
            throws IOException, YarnException {
        final String                      host   = InetAddress.getLocalHost().getHostName();
        final int                         port   = Integer.parseInt(String.valueOf(storm_conf.get(Config.MASTER_THRIFT_PORT)));
        final String                      target = host + ":" + port;
        InetSocketAddress                 addr   = NetUtils.createSocketAddr(target);
        RegisterApplicationMasterResponse resp   = rmClient.registerApplicationMaster(addr.getHostName(), port, null);

        LOG.info("Got a registration response " + resp);
        LOG.info("Max Capability " + resp.getMaximumResourceCapability());

        return resp;
    }

    public static StormAMRMClient startAppMaster2ResourceManager(Map storm_conf,
                                                                 BlockingQueue<Container> launcherQueue) {
        YarnConfiguration hadoopConf = new YarnConfiguration();
        StormAMRMClient   rmClient   = new StormAMRMClient(storm_conf, hadoopConf);

        rmClient.init(hadoopConf);
        rmClient.start();
        LOG.info("Starting launcher");

        return rmClient;
    }

    public static class StormServiceImpl implements StormService.Iface
    {


        @Override
        public String addSupervisor(String num) throws TException {
            return "您正式真正的三国无双";
        }
    }

}
