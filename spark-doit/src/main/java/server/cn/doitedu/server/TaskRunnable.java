package server.cn.doitedu.server;

import common.cn.doitedu.common.Task;

/**
 * @author malc
 * @create 2022/12/8 0008 21:21
 */
public class TaskRunnable implements Runnable{
    private Task task;

    public TaskRunnable(Task task) {
        this.task = task;
    }

    @Override
    public void run() {
        task.runTask();
    }
}
