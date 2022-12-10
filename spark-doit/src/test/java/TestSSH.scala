/**
 * @author malichun
 * @create 2022/12/09 0009 22:40
 */
object TestSSH {
    def main(args: Array[String]): Unit = {
        import cn.hutool.extra.ssh.JschUtil

        // 新加坡
        val session = JschUtil.getSession("8.219.106.195", 22, "root", "YL&p3XTQvp*mjQJwWfDG")
        //新建会话，此会话用于ssh连接到跳板机（堡垒机），此处为10.1.1.1:22//新建会话，此会话用于ssh连接到跳板机（堡垒机），此处为10.1.1.1:22

        List(
            "cdh-sg-03#8088",
            "cdh-sg-02#18088",
        ).foreach(t => {
            val arr = t.split("#")

            new Thread(() => {
                bind(arr(0),arr(1).toInt)
            }).start()
        })

        def bind(host:String, port:Int): Unit ={
            // 将堡垒机保护的内网8080端口映射到localhost，我们就可以通过访问http://localhost:8080/访问内网服务了
            JschUtil.bindPort(session, host, port, port)
        }

    }

}
