package com.yss.yarn;

/**
 * @Description storm运行时状态，以及期望运行状态
 * @author: zhangchi
 * @Date: 2017/6/7
 */
public class StormRunningState {
    private static final StormRunningState stormRunningState      = new StormRunningState();
    private volatile int                   nimbusRunningNum       = 1;
    private volatile int                   uiRunningNum           = 1;
    private volatile int                   supervisorRunningNum   = 1;
    private volatile int                   nimbusExpectRunNum     = 1;
    private volatile int                   uiExpectRunNum         = 1;
    private volatile int                   supervisorExpectRunNum = 1;

    public void increaseNimbusExpectRunNum(int num) {
        synchronized (this) {
            this.nimbusExpectRunNum += num;
        }
    }

    public void increaseNimbusRunningNum(int num) {
        synchronized (this) {
            this.nimbusRunningNum += num;
        }
    }

    public void increaseProcessExceptRunNum(StormDockerService.PROCESS process, int num) {
        if (process == StormDockerService.PROCESS.NIMBUS) {
            increaseNimbusExpectRunNum(num);
        } else if (process == StormDockerService.PROCESS.UI) {
            increaseUiExpectRunNum(num);
        } else if (process == StormDockerService.PROCESS.SUPERVISOR) {
            this.increaseSupervisorExpectRunNum(num);
        }
    }

    public void increaseProcessRunningState(StormDockerService.PROCESS process, int num) {
        if (process == StormDockerService.PROCESS.NIMBUS) {
            increaseNimbusRunningNum(num);
        } else if (process == StormDockerService.PROCESS.UI) {
            increaseUiRunningNum(num);
        } else if (process == StormDockerService.PROCESS.SUPERVISOR) {
            this.increaseSupervisorRunningNum(num);
        }
    }

    public void increaseSupervisorExpectRunNum(int num) {
        synchronized (this) {
            this.supervisorExpectRunNum += num;
        }
    }

    public void increaseSupervisorRunningNum(int num) {
        synchronized (this) {
            this.supervisorRunningNum += num;
        }
    }

    public void increaseUiExpectRunNum(int num) {
        synchronized (this) {
            this.uiExpectRunNum += num;
        }
    }

    public void increaseUiRunningNum(int num) {
        synchronized (this) {
            this.uiRunningNum += num;
        }
    }

    public static StormRunningState getInstance() {
        return stormRunningState;
    }

    public boolean isNimbusExpectRun() {
        return nimbusExpectRunNum > nimbusRunningNum;
    }

    public void setNimbusExpectRunNum(int nimbusExpectRunNum) {
        this.nimbusExpectRunNum = nimbusExpectRunNum;
    }

    public void setNimbusRunningNum(int nimbusRunningNum) {
        this.nimbusRunningNum = nimbusRunningNum;
    }

    public boolean isProcessExpectRun(StormDockerService.PROCESS process) {
        if (process == StormDockerService.PROCESS.NIMBUS) {
            return isNimbusExpectRun();
        } else if (process == StormDockerService.PROCESS.UI) {
            return isUIExpectRun();
        } else if (process == StormDockerService.PROCESS.SUPERVISOR) {
            return isSupervisorExpectRun();
        }

        return false;
    }

    public boolean isSupervisorExpectRun() {
        return supervisorExpectRunNum > supervisorRunningNum;
    }

    public void setSupervisorExpectRunNum(int supervisorExpectRunNum) {
        this.supervisorExpectRunNum = supervisorExpectRunNum;
    }

    public void setSupervisorRunningNum(int supervisorRunningNum) {
        this.supervisorRunningNum = supervisorRunningNum;
    }

    public boolean isUIExpectRun() {
        return uiExpectRunNum > uiRunningNum;
    }

    public void setUiExpectRunNum(int uiExpectRunNum) {
        this.uiExpectRunNum = uiExpectRunNum;
    }

    public void setUiRunningNum(int uiRunningNum) {
        this.uiRunningNum = uiRunningNum;
    }
}
