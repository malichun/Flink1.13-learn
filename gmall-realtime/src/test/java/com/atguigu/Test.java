package com.atguigu;

import java.util.concurrent.TimeUnit;

/**
 * @author malichun
 * @create 2022/07/20 0020 20:24
 */
public class Test {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
            System.out.println(Thread.currentThread().getName() + "\t开始运行," +
                ((Thread.currentThread().isDaemon()) ? "守护线程":"用户线程"));
        }, "t1");

        t1.start();

        // 暂停几秒钟线程
        TimeUnit.SECONDS.sleep(3);

        System.out.println(Thread.currentThread().getName() + "\t--- end 主线程");

    }
}
