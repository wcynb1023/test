/*
package org.example;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;
public class App{
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //指定输入文件路径input和输出文件路径output
        String[] otherArgs = new String[]{String.valueOf(new Path(args[0])), String.valueOf(new Path(args[1]))};
        if(otherArgs.length < 2){
            System.err.println("没有输入输出路径！");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        //通过找到给定类的来源来设置jar
        job.setJarByClass(App.class);
        job.setMapperClass(App.Map.class);
        job.setCombinerClass(App.Red.class);
        job.setReducerClass(App.Red.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class Red extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            List<String> childGroup = new ArrayList<String>();
            List<String> fatherGroup = new ArrayList<String>();

            for (Text val : values) {
                String[] str = val.toString().split(" ");
                if (str[0].charAt(0) == '1') {
                    childGroup.add(str[1]);
                } else {
                    fatherGroup.add(str[1]);
                }
                if (!childGroup.isEmpty() && !fatherGroup.isEmpty()) {
                    for (String Fa : fatherGroup) {
                        for (String Ch : childGroup) {
                            context.write(new Text(Fa), new Text(Ch));
                        }
                    }
                }
            }
        }
    }
    public  static class Map extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            context.write(new Text(values[0]), new Text("1 " + values[1]));
            context.write(new Text(values[1]), new Text("2 " + values[2]));
        }
    }
}
*/
