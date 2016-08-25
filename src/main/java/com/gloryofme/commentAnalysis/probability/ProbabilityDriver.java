package com.gloryofme.commentAnalysis.probability;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gloryofme.commentAnalysis.Constant;
import com.gloryofme.commentAnalysis.dic.DictionaryFetcher;
import com.gloryofme.commentAnalysis.segment.SegmentDriver;
import com.gloryofme.commentAnalysis.segment.SegmentDriver.SegmentMapper;
import com.gloryofme.commentAnalysis.tools.HDFSUtil;

/**
 *概率计算
 */
public class ProbabilityDriver {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SegmentDriver.class);

	public static class ProbaMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private static long totalCount = 0;

		private static String[] stopWords = null;

		private static Set<String> stopWordSet = new HashSet<String>();

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			totalCount = conf.getLong("totalCount", 0);
			stopWords = conf.getStrings("stopWords", new String[] { " " });
			for (int i = 0; i < stopWords.length; i++)
				stopWordSet.add(stopWords[i]);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			Text k = new Text();
			LongWritable v = new LongWritable(1);
			HashSet<String> lineSet = new HashSet<String>();
			for (String word : words) {
				// System.out.println(feaDic.size());
				// if(!feaDic.contains(word) && !senDic.contains(word))
				// TODO 所有的词进行统计 一个句子中出现多次为1次
				if (lineSet.contains(word) || stopWordSet.contains(word))
					continue;
				lineSet.add(word);
				k.set(word);
				context.write(k, v);
			}
		}
	}

	public static class ProbaReducer extends
			Reducer<Text, LongWritable, Text, FloatWritable> {

		private FloatWritable v = new FloatWritable();

		private static long totalCount = 0;

		private static String[] stopWords = null;

		private static final long M = 0L;

		private static final int V = 2;

		private static final int zoom = 100;

		@Override
		protected void setup(
				Reducer<Text, LongWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			totalCount = conf.getLong("totalCount", 0);
			stopWords = conf.getStrings("stopWords");
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			float pro = (float) ((count + 1) / ((totalCount * 1.0) + M + V) * zoom);
			v.set(pro);
			context.write(key, v);
		}
	}

	public void run() throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		conf.setStrings(Constant.STOP_WORDS);
		long posCount = getCount(conf, Constant.HDFS_PREFIX
				+ "/comment/proba/positive/count/part-r-00000");
		if (posCount == 0)
			return;
		conf.setLong("totalCount", posCount);
		String inPath = Constant.HDFS_PREFIX + "/comment/training/seg/positive";
		String outPath = Constant.HDFS_PREFIX + "/comment/proba/res/positive/";
		if (HDFSUtil.exits(conf, outPath))
			HDFSUtil.deleteFileRecursive(conf, outPath);
		generateProba(conf, inPath, outPath);
		//
		long negCount = getCount(conf, Constant.HDFS_PREFIX
				+ "/comment/proba/negative/count/part-r-00000");
		if (negCount == 0)
			return;
		conf.setLong("totalCount", negCount);
		inPath = Constant.HDFS_PREFIX + "/comment/training/seg/negative";
		outPath = Constant.HDFS_PREFIX + "/comment/proba/res/negative/";
		if (HDFSUtil.exits(conf, outPath))
			HDFSUtil.deleteFileRecursive(conf, outPath);
		generateProba(conf, inPath, outPath);
	}

	public void generateProba(Configuration conf, String inPath, String outPath) {
		try {
			Job job = Job.getInstance(conf, "probaJob");
			job.setJarByClass(ProbabilityDriver.class);
			job.setMapperClass(ProbaMapper.class);
			job.setReducerClass(ProbaReducer.class);
			job.setInputFormatClass(TextInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			if (HDFSUtil.exits(conf, outPath)) {
				HDFSUtil.deleteFileRecursive(conf, outPath);
			}

			FileInputFormat.addInputPath(job, new Path(inPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));

			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long getCount(Configuration conf, String path) {
		long count = 0;
		try {
			String content = HDFSUtil.readFile(conf, path);
			String[] items = content.split("\t");
			count = Long.parseLong(items[1].replaceAll("\\n", ""));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return count;
	}
}
