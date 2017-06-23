package com.floodCtr.job;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.floodCtr.FloodContrSubScheduler;

import com.google.common.collect.Lists;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class JobRegisterPubTable {
    private static final Logger LOG = LoggerFactory.getLogger(JobRegisterPubTable.class);

    /**
     * 任务发布注册表，每一个发布了的任务，都在此
     */
    private static ConcurrentHashMap<String, FloodJob> jobRegisterTable = new ConcurrentHashMap<>();

    /**
     * 注册任务
     * @param floodContrJob
     */
    public static synchronized void registerJob(FloodJob floodContrJob) {
        jobRegisterTable.put(floodContrJob.getJobId(), floodContrJob);
    }

    /**
     * 移除任务
     * @param id
     */
    public static synchronized void removeJob(String id) {
        jobRegisterTable.remove(id);
    }

    /**
     * 获取所有任务
     * @return
     */
    public static List<FloodJob> getAllFloodContrJob() {
        if (MapUtils.isEmpty(jobRegisterTable)) {
            LOG.info("jobpool is empy");

            return Lists.newArrayList();
        }

        Collection<FloodJob> contrJobs = jobRegisterTable.values();
        List<FloodJob>       list      = Lists.newArrayList();
        Iterator             it        = contrJobs.iterator();

        while (it.hasNext()) {
            FloodJob floodContrJob = (FloodJob) it.next();

            LOG.info("found job " + floodContrJob.getJobId());
            list.add(floodContrJob.clone());
        }

        return list;
    }
}
