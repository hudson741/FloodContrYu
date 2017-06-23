package com.floodCtr.publish;

/**
 * @Description 描述任务发布的紧急程度
 * @author: zhangchi
 * @Date: 2017/6/22
 */
public enum PRIORITY {
    LOW(1), DEFAULT_PRIORITY(2), HIGH(3);

    private int code;

    PRIORITY(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
