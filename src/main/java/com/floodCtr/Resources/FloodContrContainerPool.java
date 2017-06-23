package com.floodCtr.Resources;

import org.apache.hadoop.yarn.api.records.Container;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class FloodContrContainerPool {

    public final static BlockingQueue<Container> containerPool = new LinkedBlockingQueue<>();

}
