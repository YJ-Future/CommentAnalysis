package com.gloryofme.commentAnalysis.classify;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ICTCLAS.I3S.AC.Segmenter;

import com.gloryofme.commentAnalysis.Constant;
import com.gloryofme.commentAnalysis.dic.DictionaryFetcher;
import com.gloryofme.commentAnalysis.segment.SegmentDriver;
import com.gloryofme.commentAnalysis.tools.HDFSUtil;

/**
 * 分类
 */
public class ClassifyDriver {

	public static class ClassifyMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		// 正面情感先验概率
		private HashMap<String, Float> posProbs = new HashMap<String, Float>();
		// 负面情感先验概率
		private HashMap<String, Float> negProbs = null;

		private Text v = new Text();
		// 放大因子
		private float zoom = 100;

		private float M = 0L;

		private long V = 2;

		private long posCount = 0;

		private long negCount = 0;
		// 停用词
		private HashSet<String> stopWordSet = new HashSet<>();

		private String[] stopWords = null;

		// 特征词典
		private HashSet<String> feaDict = null;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String posProbsPath = conf.get("posProbsPath");
			String negProbsPath = conf.get("negProbsPath");

			posProbs = ProbabilityFetcher.getProbability(conf, posProbsPath);
			negProbs = ProbabilityFetcher.getProbability(conf, negProbsPath);
			//
			posCount = conf.getLong("posCount", 0);
			negCount = conf.getLong("negCount", 0);

			stopWords = conf.getStrings("stopWords");
			for (String word : stopWords) {
				stopWordSet.add(word);
			}
			//
			feaDict = DictionaryFetcher.getFeaDic(conf);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String orignLine = line;
			Segmenter segmenter = new Segmenter();
			try {
				line = segmenter.getWords(line);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String lineWithPos = line;//带有词性标注的分词结果
			line = line.replaceAll("/[\\w]+\\s", " ");
			String lineNoPos = line;//不带词性标注的分词结果
			StringBuilder strBuilder = new StringBuilder();
			//
			boolean totalFlag = analyseSentiment(line,posProbs,negProbs,stopWordSet,posCount,negCount,zoom,M,V);
			strBuilder.append("总体:");
			strBuilder.append(totalFlag?"正面 |":"负面|");
			boolean feaFlag = false;
			//计算特征词情感倾向
			String[] frags = lineNoPos.split("[\\s|,|.|!|~|。|，|~|！]");
			//分句情感分析
			HashSet<String> feaInFrag = new HashSet<>();//去重
			for(int i=0;i<frags.length;i++){
				feaInFrag.clear();
				//分句情感分类
				feaFlag = analyseSentiment(frags[i], posProbs, negProbs, stopWordSet, posCount, negCount, zoom, M, V);
				String [] words = frags[i].split(" ");
				for(String word:words){
					if(feaDict.contains(word)&& !feaInFrag.contains(word)){
						strBuilder.append(word+":");
						strBuilder.append(feaFlag?"正面|":"负面|");
						feaInFrag.add(word);
					}
				}
			}
			System.out.println(strBuilder.toString());
			v.set(strBuilder.toString());
			context.write(value, v);
		}
	}

	public static boolean analyseSentiment(String comment,
			HashMap<String, Float> posProbs, HashMap<String, Float> negProbs,
			HashSet<String> stopWordSet, long posCount, long negCount,
			float zoom, float M, long V) {
		String[] items = comment.split(" ");
		float posPro = 1L;
		float negPro = 1L;
		for (String item : items) {
			if (stopWordSet.contains(item))
				continue;
			// TODO 不在先验概率统计中 如何处理
			if (posProbs.get(item) == null)// 过滤掉的停用词
				posPro *= (float) (1.0 / (posCount + M + V) * zoom);
			else
				posPro *= posProbs.get(item);
			if (negProbs.get(item) == null)
				negPro *= (float) (1.0 / (negCount + M + V) * zoom);
			else
				negPro *= negProbs.get(item);
		}
		System.out.println("正面" + posPro);
		System.out.println("负面" + negPro);
		if (posPro > negPro)
			return true;
		return false;
	}

	public static class ClassifyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values)
				context.write(key, value);
		}
	}

	public void classify(Configuration conf, String inPath, String outPath)
			throws IOException {
		Path in = new Path(inPath);
		Path out = new Path(outPath);
		if (HDFSUtil.exits(conf, outPath)) {
			HDFSUtil.deleteFileRecursive(conf, outPath);
		}
		Job job = Job.getInstance(conf, "classifyJob");
		job.setMapperClass(ClassifyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, in);
		//
		job.setReducerClass(ClassifyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, out);
			
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() throws IOException {
		Configuration conf = new Configuration();
		long posCount = getCount(conf, Constant.HDFS_PREFIX
				+ "/comment/proba/positive/count/part-r-00000");
		long negCount = getCount(conf, Constant.HDFS_PREFIX
				+ "/comment/proba/negative/count/part-r-00000");
		conf.setLong("posCount", posCount);
		conf.setLong("negCount", negCount);
		conf.setStrings("stopWords",Constant.STOP_WORDS);
		conf.set("posProbsPath", Constant.HDFS_PREFIX
				+ "/comment/proba/res/positive/part-r-00000");
		conf.set("negProbsPath", Constant.HDFS_PREFIX
				+ "/comment/proba/res/negative/part-r-00000");
		classify(conf, Constant.HDFS_PREFIX + "/comment/test/sample1",
				Constant.HDFS_PREFIX + "/comment/test/sample_result1");
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
