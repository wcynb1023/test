
package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class App2{
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
    public static class Red extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static class Map extends Mapper<Object, Text, Text, IntWritable>{
        private Text word = new Text();
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(" ");
            for(String word:values){
                context.write(new Text(word) , new IntWritable(1));
            }
        }
    }
}

