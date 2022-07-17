package app.function;

import bean.OrderWide;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.DimUtil;
import utils.ThreadPoolUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/**
 * 维度异步查询
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T>{ // <IN,OUT>

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 线程池
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }



    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 获取查询的主键
                    String key = getKey(input);
                    // 查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    // 补充维度信息
                    if(dimInfo!=null){
                        join(input, dimInfo);
                    }

                    // 将数据输出
                    resultFuture.complete(Collections.singleton(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 超时, 可以在这儿发起一次请求
        System.out.println("Timeout:" + input);

    }
}
