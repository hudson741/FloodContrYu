package com.floodCtr;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


/**
 * @Description
 * @author: zhangchi
 * @Date: 2017/7/5
 */
public class FtpUtil {

    private Logger logger        = LoggerFactory.getLogger(FtpUtil.class);

    private FTPClient ftpClient;

    private String userName;

    private String password;

    private String addr;

    private String port;

    public FtpUtil(String addr,String port,String userName,String password) throws IOException {
        this.addr = addr;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.init();
    }

    public void upload(String path,File file) throws IOException {
        if(!ftpClient.isConnected()){
            init();
        }
        BufferedInputStream fiStream = new BufferedInputStream(new FileInputStream(file));
        ftpClient.pasv();
        ftpClient.changeWorkingDirectory(path);
        ftpClient.enterLocalPassiveMode();
        ftpClient.storeFile(file.getName(), fiStream);
        fiStream.close();
    }

    public void writes(String path,String fileName, InputStream inputStream) throws IOException {
        if(!ftpClient.isConnected()){
            init();
        }
        createDirecroty(path);
        changeWorkingDirectory(path);
        ftpClient.storeFile(fileName, inputStream);
        inputStream.close();
    }

    public long getFileSize(String filePath,String fileName) throws IOException {
        if(!ftpClient.isConnected()){
            init();
        }
        changeWorkingDirectory(filePath);
        return ftpClient.mlistDir(filePath+"/"+fileName)[0].getSize();
    }

    public long getFtpFileTimeStamp(String filePath) throws IOException {
        if(!ftpClient.isConnected()){
            init();
        }
        FTPFile[] ftpFile = ftpClient.listFiles(filePath);
        if(ftpFile == null || ftpFile.length == 0){
            return 0l;
        }

        long timeStamp = ftpFile[0].getTimestamp().getTimeInMillis();

        return timeStamp;
    }

    public void disconnect(){
        try {
            ftpClient.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //判断ftp服务器文件是否存在
    public boolean existFile(String path) throws IOException {
        boolean flag = false;
        FTPFile[] ftpFileArr = ftpClient.listFiles(path);
        if (ftpFileArr.length > 0) {
            flag = true;
        }
        return flag;
    }

    public boolean changeWorkingDirectory(String directory) {
        boolean flag = true;
        try {
            flag = ftpClient.changeWorkingDirectory(directory);
            if (flag) {
                logger.debug("进入文件夹" + directory + " 成功！");

            } else {
                logger.debug("进入文件夹" + directory + " 失败！");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return flag;
    }

    public boolean createDirecroty(String remote) throws IOException {
        boolean success = true;
        String directory = remote + "/";
//        String directory = remote.substring(0, remote.lastIndexOf("/") + 1);
        // 如果远程目录不存在，则递归创建远程服务器目录
        if (!directory.equalsIgnoreCase("/") && !changeWorkingDirectory(new String(directory))) {
            int start = 0;
            int end = 0;
            if (directory.startsWith("/")) {
                start = 1;
            } else {
                start = 0;
            }
            end = directory.indexOf("/", start);
            String path = "";
            String paths = "";
            while (true) {

                String subDirectory = new String(remote.substring(start, end).getBytes("GBK"), "iso-8859-1");
                path = path + "/" + subDirectory;
                if (!existFile(path)) {
                    if (makeDirectory(subDirectory)) {
                        changeWorkingDirectory(subDirectory);
                    } else {
                        logger.debug("创建目录[" + subDirectory + "]失败");
                        changeWorkingDirectory(subDirectory);
                    }
                } else {
                    changeWorkingDirectory(subDirectory);
                }

                paths = paths + "/" + subDirectory;
                start = end + 1;
                end = directory.indexOf("/", start);
                // 检查所有目录是否创建完毕
                if (end <= start) {
                    break;
                }
            }
        }
        return success;
    }

    //创建目录
    public boolean makeDirectory(String dir) {
        boolean flag = true;
        try {
            flag = ftpClient.makeDirectory(dir);
            if (flag) {
                logger.debug("创建文件夹" + dir + " 成功！");

            } else {
                logger.debug("创建文件夹" + dir + " 失败！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    public void setFtpClient(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    private void init() throws IOException {
        ftpClient = new FTPClient();
        int reply;

        ftpClient.connect(addr, Integer.parseInt(port));
        ftpClient.login(userName, password);
        ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
        reply = ftpClient.getReplyCode();

        if (!FTPReply.isPositiveCompletion(reply)) {
            ftpClient.disconnect();
        }

        ftpClient.enterLocalPassiveMode();
    }


    public String getRemoteFtpServerAddress(){

        if(StringUtils.isNotEmpty(userName)){
            return "ftp://"+userName+":"+password+"@"+addr+":"+port;
        }

        return "ftp://"+addr+":"+port;
    }

    public static void main(String[] args) throws IOException {
        FtpUtil ftpUtil = new FtpUtil("zhangc1","9099","yarn","yarn");
        System.out.println(ftpUtil.getFileSize("dockershell"+ Path.SEPARATOR+"container_e03_1499334976379_0006_01_000002","execute.sh"));
    }

}
