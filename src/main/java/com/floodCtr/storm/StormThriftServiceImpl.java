package com.floodCtr.storm;


import java.util.List;

import com.floodCtr.YarnClient;
import com.floodCtr.rpc.FloodContrThriftServiceImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import com.floodCtr.monitor.FloodContrRunningMonitor;
import com.floodCtr.monitor.FloodJobRunningState;
import com.floodCtr.publish.FloodContrJobPubProxy;

import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/28
 */
public class StormThriftServiceImpl extends FloodContrThriftServiceImpl implements StormThriftService.Iface {
    private static final Logger   logger = LoggerFactory.getLogger(StormThriftServiceImpl.class);

    public StormThriftServiceImpl(YarnClient yarnClient, FloodContrJobPubProxy floodContrJobPubProxy) {
        super(yarnClient, floodContrJobPubProxy);
    }

    @Override
    public String getStormNimbus() throws TException {
        List<String>               result = Lists.newArrayList();
        List<FloodJobRunningState> list   = FloodContrRunningMonitor.getFloodJobRunningState();

        if (CollectionUtils.isEmpty(list)) {
            return "";
        }

        for (FloodJobRunningState floodJobRunningState : list) {
            if (floodJobRunningState.getFloodJob().getBusinessTag().equals("nimbus")) {
                result.add(floodJobRunningState.getRunIp()+":"+
                           floodJobRunningState.getFloodJob().getDockerCMD().getContainerName() + ":"
                           +floodJobRunningState.getFloodJob().getDockerCMD().getIp()+":"
                            + "9005");
            }
        }

        return JSONObject.toJSONString(result);
    }

    @Override
    public String getStormUi() throws TException {
        List<FloodJobRunningState> list = FloodContrRunningMonitor.getFloodJobRunningState();

        if (CollectionUtils.isEmpty(list)) {
            return "";
        }

        for (FloodJobRunningState floodJobRunningState : list) {
            System.out.println("getStormUI "+JSONObject.toJSONString(floodJobRunningState));
            if (floodJobRunningState.getFloodJob().getBusinessTag().equals("ui") && floodJobRunningState.getRunningState() == FloodJobRunningState.RUNNING_STATE.RUNNING) {
                return floodJobRunningState.getRunIp() + ":9092";
            }
        }

        return null;
    }
}
