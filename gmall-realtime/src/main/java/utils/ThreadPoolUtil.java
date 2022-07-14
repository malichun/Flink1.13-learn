package utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类
 * 双检索的方式
 * @author malichun
 * @create 2022/07/13 0013 23:36
 */
public class ThreadPoolUtil {

    private static volatile ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(){}


    // double check locking
    public static ThreadPoolExecutor getThreadPool() {

        if(threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class) {
                if(threadPoolExecutor == null) {
                    // FIXME maxPoolSize: coreSize = 4, 当队列满了才会创建第5个线程!!!
                    threadPoolExecutor = new ThreadPoolExecutor(8,
                        16,
                        60,
                        TimeUnit.SECONDS,
                        new LinkedBlockingDeque<>());
                }
            }

        }
        return threadPoolExecutor;
    }
}
