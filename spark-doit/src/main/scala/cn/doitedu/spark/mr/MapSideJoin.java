package cn.doitedu.spark.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author malichun
 * @create 2022/11/28 0028 23:22
 */
public class MapSideJoin {
    public static class XMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        Map<Integer,String> map = new HashMap<Integer, String>();
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("./name.txt"));
            String line = null;
            while((line = br.readLine())!=null){
                String[] split = line.split(",");
                map.put(Integer.parseInt(split[0]), split[1]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            // 通过map
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(XMapper.class);

        // 添加缓存文件
        job.addCacheFile(new URI("hdfs://doit01:8020/abc/names.txt"));

        // 会发送缓存文件到每个task的本地目录
        job.waitForCompletion(true);
    }
}
