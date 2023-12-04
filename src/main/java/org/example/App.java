
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
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class Red extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException{
          //context.write(new Text("testKey"), new Text("testValue"));
            //System.out.println(key.toString()+": ");
            double sum = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            for(Text val :  values) {
                String[] word = val.toString().split(" ");
                double a = Double.valueOf(word[1].toString());
                if(sum < a){
                    sum = a;
                }
                if(min > a){
                    min = a;
                }
            }
            System.out.println(key.toString()+": "+sum+" "+min);
            context.write(key,new Text(": "+sum+" "+min));
        }
    }
    public static class Map extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException{
            String[] values = value.toString().split(" ");
            context.write(new Text(values[0]) , new Text(values[1]+" "+values[3]));
        }
    }
}

