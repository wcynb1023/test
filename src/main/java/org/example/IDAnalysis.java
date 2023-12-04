package org.example;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IDAnalysis {

    public static class IDAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String id = line.substring(0, 18);
            String provinceCode = id.substring(0, 2);
            String birthDate = id.substring(6, 14);
            String genderDigit = id.substring(16, 17);

            // 统计省份
            word.set("Province: " + provinceCode);
            context.write(word, one);

            // 计算年龄
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            try {
                Date birth = dateFormat.parse(birthDate);
                Calendar cal = Calendar.getInstance();
                int currentYear = cal.get(Calendar.YEAR);
                cal.setTime(birth);
                int birthYear = cal.get(Calendar.YEAR);
                int age = currentYear - birthYear;

                // 统计年龄段
                if (age >= 0 && age <= 10) {
                    word.set("Age Group: 0-10");
                } else if (age <= 20) {
                    word.set("Age Group: 11-20");
                } else if (age <= 30) {
                    word.set("Age Group: 21-30");
                } else if (age <= 40) {
                    word.set("Age Group: 31-40");
                } else if (age <= 50) {
                    word.set("Age Group: 41-50");
                } else if (age <= 60) {
                    word.set("Age Group: 51-60");
                } else if (age <= 70) {
                    word.set("Age Group: 61-70");
                } else {
                    word.set("Age Group: 70+");
                }
                //System.out.println(word.toString());
                context.write(word, one);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            // 统计性别
            if (Integer.parseInt(genderDigit) % 2 == 0) {
                word.set("Gender: Female");
            } else {
                word.set("Gender: Male");
            }
            context.write(word, one);
        }
    }

    public static class IDAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            System.out.println(key.toString()+ result.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ID Analysis");
        job.setJarByClass(IDAnalysis.class);
        job.setMapperClass(IDAnalysisMapper.class);
        job.setCombinerClass(IDAnalysisReducer.class);
        job.setReducerClass(IDAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}