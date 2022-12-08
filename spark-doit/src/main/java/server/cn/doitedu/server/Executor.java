package server.cn.doitedu.server;

import common.cn.doitedu.common.Task;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 是一个进程
 */
public class Executor {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(16200);

        Socket socket = serverSocket.accept();
        InputStream in = socket.getInputStream();

        ObjectInputStream oin = new ObjectInputStream(in);

        Task task = (Task) oin.readObject();

        oin.close();
        in.close();
        socket.close();

        // 创建一个线程去运行这个收到的task
        new Thread(new TaskRunnable(task)).start();

    }
}
