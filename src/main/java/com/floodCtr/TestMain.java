package com.floodCtr;

import com.floodCtr.job.FloodJob;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class TestMain {

    public static void main(String[] args) throws IOException, InterruptedException {


    }


    static class User{

        public User(String name,String age){
            this.name = name;
            this.age = age;
        }
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        String name;

        String age;
    }
}
