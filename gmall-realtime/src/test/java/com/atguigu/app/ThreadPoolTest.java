package com.atguigu.app;

import utils.ThreadPoolUtil;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author malichun
 * @create 2022/07/14 0014 0:02
 */
public class ThreadPoolTest {
    public static void main(String[] args) {
        ThreadPoolExecutor threadPool = ThreadPoolUtil.getThreadPool();
        for (int i = 0; i < 10; i++) {
            threadPool.submit(() ->{

                System.out.println(Thread.currentThread().getName() + ":atguigu");
                try {
                    Thread.sleep(1000000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

    }
}
