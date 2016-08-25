package com.gloryofme.commentAnalysis.probability;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.gloryofme.commentAnalysis.tools.HDFSUtil;

/**
 * 计数统计
 */
public class CountDriver {

    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println(value.toString());
            Text k = new Text("count");
            context.write(k, new LongWritable(1));
        }

    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            LongWritable v = new LongWritable(0);
            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            v.set(count);
            context.write(key, v);
        }
    }

    public void run() {
        Configuration conf = new Configuration();
        String inPath = Constant.HDFS_PREFIX + "/comment/training/seg/positive";
        String outPath = Constant.HDFS_PREFIX + "/comment/proba/positive/count";
        count(conf, "posCountJob", inPath, outPath);
        inPath = Constant.HDFS_PREFIX + "/comment/training/seg/negative";
        outPath = Constant.HDFS_PREFIX + "/comment/proba/negative/count";
        count(conf, "negCountJob", inPath, outPath);
    }

    public void count(Configuration conf, String name, String inPath, String outPath) {
        Path in = new Path(inPath);
        Path out = new Path(outPath);
        try {
            Job job = Job.getInstance(conf, name);
            job.setJarByClass(CountDriver.class);
            job.setMapperClass(CountMapper.class);
            job.setReducerClass(CountReducer.class);
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            if (HDFSUtil.exits(conf, outPath)) {
                HDFSUtil.deleteFileRecursive(conf, outPath);
            }

            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);

            try {
                job.waitForCompletion(true);
            } catch (ClassNotFoundException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
