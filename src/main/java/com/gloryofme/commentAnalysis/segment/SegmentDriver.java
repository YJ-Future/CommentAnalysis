package com.gloryofme.commentAnalysis.segment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ICTCLAS.I3S.AC.Segmenter;

/**
 * 分词
 */
public class SegmentDriver {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SegmentDriver.class);
	private static Segmenter segmenter = new Segmenter();

	public static class SegmentMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		Text k = new Text();
		NullWritable v = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			// 替换模式
			line = line.replaceAll("闪退", "昶");
			line = line.replaceAll("艹", "昶");
			line = line.replaceAll("不(是|太|很)*(快|好|给力|放心|满意|习惯|好用|耐用|强|稳定|支持)",
					"昶");
			line = line.replaceAll("(太|很)*(小|差)", "昶");
			line = line.replaceAll("有问题", "昶");
			line = line.replaceAll("渣", "昶");

			line = line.replaceAll("赞", "甄");
			line = line.replaceAll("不错", "甄");
			line = line.replaceAll("哦", "甄");
			line = line.replaceAll("哒", "甄");
			line = line.replaceAll("牛叉", "甄");
			line = line.replaceAll("快", "甄");
			line = line.replaceAll("好", "甄");
			line = line.replaceAll("真棒", "甄");
			line = line.replaceAll("棒", "甄");
			line = line.replaceAll("很棒", "甄");
			line = line.replaceAll("(好用|耐用|给力|愉快|灵敏|强|齐全|稳定|支持)", "甄");
			line = line.replaceAll("没(有)*(的)*说(的)*", "甄");
			line = line.replaceAll("屏幕大", "甄");

			// 分词
			try {
				line = segmenter.getWords(line);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// 取出 词性标注
			// TODO //是否需要替换成空格 分词结束已经会有空格
			line = line.replaceAll("/[\\w]+\\s", " ");
			k.set(line);
			context.write(k, v);
		}
	}

	public static class SegmentReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		private NullWritable v = NullWritable.get();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (NullWritable value : values)
				context.write(key, v);
		}

	}

	public void segment(Configuration conf, String inPath, String outPath) {
		conf = new Configuration();
		Path in = new Path(inPath);
		Path out = new Path(outPath);
		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(out)) {
				fs.delete(out);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Job job = Job.getInstance(conf, "segmentJob");
			job.setJarByClass(SegmentDriver.class);
			job.setMapperClass(SegmentMapper.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setReducerClass(SegmentReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);

			job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run(String inPath,String outPath) {
		Configuration conf = new Configuration();
		segment(conf, inPath, outPath);
	}
}
