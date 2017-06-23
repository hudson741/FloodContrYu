package com.floodCtr;

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
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public class TestMain {

    public static void main(String[] args) throws IOException {

        URL url =  Resources.getResource("net.shell");


        List<String> lines = Files.readLines(new File(url.getFile()), Charset.defaultCharset());

        for(String line:lines){
            line = StringUtils.replace(line,"$containerName","ui-3");
            System.out.println(line);
        }

    }
}
