package driver.cn.doitedu.driver;

import common.cn.doitedu.common.Task;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * @author malc
 * @create 2022/12/8 0008 21:24
 */
public class Driver {
    public static void main(String[] args) throws IOException {
        // 生成2个Task, 发给两个executor去执行
        Task task1 = new Task("task-0");

        Task task2 = new Task("task-0");

        Socket socket1 = new Socket("doit02", 16200);
        Socket socket2 = new Socket("doit03", 16200);

        // 发送对象
        OutputStream out1 = socket1.getOutputStream();
        ObjectOutputStream oos1 = new ObjectOutputStream(out1);
        oos1.writeObject(task1);

        OutputStream out2 = socket1.getOutputStream();
        ObjectOutputStream oos2 = new ObjectOutputStream(out2);
        oos2.writeObject(task2);

        out1.close();
        oos1.close();
        socket1.close();

        out2.close();
        oos2.close();
        socket2.close();

    }
}
