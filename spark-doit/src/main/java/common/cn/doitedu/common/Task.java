package common.cn.doitedu.common;

import java.io.Serializable;

/**
 * 公共的任务
 */
public class Task implements Serializable {

    private String taskId;

    public Task(String taskId){
        this.taskId = taskId;
    }

    public void runTask(){
        while(true){
            System.out.println("hahaha. task 换了的跑起来了");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
