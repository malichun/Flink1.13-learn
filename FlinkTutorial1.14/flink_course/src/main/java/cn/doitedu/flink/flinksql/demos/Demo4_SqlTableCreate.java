package cn.doitedu.flink.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 带SQL表名的表创建
 * 各种方式
 */
public class Demo4_SqlTableCreate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // TODO 1.通过构建 TableDescriptor 来创建一个有名表
        tenv.createTable("table_a",
            TableDescriptor
                .forConnector("filesystem")
                .schema(Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("age", DataTypes.INT())
                    .column("gender", DataTypes.STRING())
                    .build())
                .format("csv")
                .option("path", "file:///D:\\projects\\learn_projects\\flink-learn\\FlinkTutorial1.14\\flink_course\\data\\sqldemo\\a.txt")
                .option("csv.ignore-parse-errors", "true")
                .option("csv.allow-comments", "true")
                .build());

        tenv.executeSql("select * from table_a")/*.print()*/;
        tenv.executeSql("select gender, max(age) as max_age from table_a group by gender")/* .print() */;

        // TODO 2. 从一个DataStream上创建"有名" 的视图

        DataStreamSource<String> stream1 = env.socketTextStream("hadoop102", 6666);

        SingleOutputStreamOperator<Person> javaBeanStream = stream1.map(new MapFunction<String, Person>() {
                @Override
                public Person map(String s) throws Exception {
                    String[] split = s.split(",");
                    Person person = new Person();
                    person.id = Integer.parseInt(split[0]);
                    person.name = split[1];
                    person.age = Integer.parseInt(split[2]);
                    person.gender = split[3];
                    return person;
                }
            });

        tenv.createTemporaryView("t_person", javaBeanStream);
        tenv.executeSql("select gender,max(age) as max_age from t_person group by gender")/*.print()*/;


        // TODO 3. 从一个已经存在的Table对象 获取"有名" 的视图
        Table table_a = tenv.from("table_a");
        tenv.createTemporaryView("table_b", table_a);
        tenv.executeSql("select * from table_b").print();


        // env.execute();
    }

    @Data
    public static class Person {
        public int id;
        public String name;
        public int age;
        public String gender;

    }
}
