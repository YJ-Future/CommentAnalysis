package com.gloryofme.commentAnalysis.dic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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

import com.gloryofme.commentAnalysis.Constant;
import com.gloryofme.commentAnalysis.probability.CountDriver.CountMapper;
import com.gloryofme.commentAnalysis.probability.CountDriver.CountReducer;
import com.gloryofme.commentAnalysis.tools.HDFSUtil;

import ICTCLAS.I3S.AC.Segmenter;

/**
 * 更新词典
 */

public class UpdateDictDriver {

	/**
	 * 单词
	 */
	public static class Word {
		public String value;// 单词字符串
		public char pos;// 词性
	}

	/**
	 * 名词短语
	 */
	public static class NounPhrase extends Word {
		int f_no;// 开始位置
		int l_no;// 结束位置
	}

	/**
	 * 形容词
	 */
	public static class Adj extends Word {
		int indexNo;
		int no;
	}

	public static class UpdateDictMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private static ArrayList<Word> words;

		private static HashSet<String> feaDic;

		private static HashSet<String> sentiDic;

		private Text k = new Text();

		private NullWritable v = NullWritable.get();

		private String type;// 产生词典的类型 特征词 | 情感词

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			feaDic = DictionaryFetcher.getFeaDic(conf);
			sentiDic = DictionaryFetcher.getSentiDic(conf);
			type = conf.get("type");

		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// 使用的直接是训练集 未经过分词处理
			String line = value.toString();
			HashSet<String> newSentis = getNewWords(line, type, feaDic,
					sentiDic);
			for (String senti : newSentis) {
				k.set(senti);
				context.write(k, v);
			}
			// TODO暂时没有封装 名词-形容词 后面需要再添加
		}
	}

	public static class UpdateDictReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		NullWritable v = NullWritable.get();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, v);
		}

	}

	public static HashSet<String> getNewWords(String line, String type,
			HashSet<String> feaDic, HashSet<String> sentiDic) {
		ArrayList<Word> words = new ArrayList<>();
		line = line.trim();
		// 分词
		Segmenter segmenter = new Segmenter();
		try {
			line = segmenter.getWords(line);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] wordsWithPos = line.split(" ");
		String[] items;
		int wordNo = 0;
		int indexNo = 0;
		HashMap<Integer, Integer> indexMap = new HashMap<>();
		List<Adj> adjs = new ArrayList<Adj>();
		String lineOfPos = "";
		List<NounPhrase> nounPhrases = new ArrayList<NounPhrase>();
		// 遍历数组 抽取 1.所有单词2.名词单词 2.到形容词3.单词词性字符串 (包括 形容词a,名词n,语气词d,其他词o)
		for (int i = 0; i < wordsWithPos.length; i++) {
			Word word = new Word();
			items = wordsWithPos[i].split("/");
			if (items.length != 2)
				continue;
			word.value = items[0];
			if (items[1].contains("n")) {
				word.pos = 'n';
				lineOfPos += 'n';
			} else if (items[1].contains("a")) {
				word.pos = 'a';
				lineOfPos += 'a';
				Adj adj = new Adj();
				adj.no = wordNo;
				adj.indexNo = indexNo;
				adj.value = items[0];
				adjs.add(adj);
			} else if (items[1].contains("d")) {
				word.pos = 'd';
				lineOfPos += 'd';
			} else {
				word.pos = 'o';
				lineOfPos += 'o';
			}
			words.add(word);
			// onlyWordLine += word.value;
			for (int j = 0; j < word.value.length(); j++) {
				indexMap.put(indexNo, wordNo);
				indexNo++;
			}
			wordNo++;
		}
		// 获取名词短语 nounPhrases
		int indexCount = -1;
		// 获取 n
		String tempLine = lineOfPos;
		while ((indexCount = tempLine.indexOf("n", indexCount + 1)) >= 0) {
			NounPhrase nounPhrase = new NounPhrase();
			nounPhrase.value = words.get(indexCount).value;
			nounPhrase.f_no = indexCount;
			nounPhrase.l_no = indexCount;
			nounPhrases.add(nounPhrase);
		}
		// 获取 nn
		while ((indexCount = lineOfPos.indexOf("nn", indexCount + 1)) >= 0) {
			NounPhrase nounPhrase = new NounPhrase();
			nounPhrase.value = words.get(indexCount).value
					+ words.get(indexCount + 1).value;
			nounPhrase.f_no = indexCount;
			nounPhrase.l_no = indexCount + 1;
			nounPhrases.add(nounPhrase);
		}
		// 获取 nnn
		while ((indexCount = lineOfPos.indexOf("nnn", indexCount + 1)) >= 0) {
			NounPhrase nounPhrase = new NounPhrase();
			nounPhrase.value = words.get(indexCount).value
					+ words.get(indexCount + 1).value
					+ words.get(indexCount + 2).value;
			nounPhrase.f_no = indexCount;
			nounPhrase.l_no = indexCount + 2;
			nounPhrases.add(nounPhrase);
		}
		// 获取 ndn
		while ((indexCount = lineOfPos.indexOf("ndn", indexCount + 1)) >= 0) {
			NounPhrase nounPhrase = new NounPhrase();
			nounPhrase.value = words.get(indexCount).value
					+ words.get(indexCount + 1).value
					+ words.get(indexCount + 2).value;
			nounPhrase.f_no = indexCount;
			nounPhrase.l_no = indexCount + 2;
			nounPhrases.add(nounPhrase);
		}

		// 根据情感次词典获取特征词
		List<Adj> sentiInSen = new ArrayList<Adj>();
		HashSet<String> sentiStrInSen = new HashSet<String>();
		for (int i = 0; i < adjs.size(); i++) {
			if (sentiDic.contains(adjs.get(i).value)) {
				sentiInSen.add(adjs.get(i));
				sentiStrInSen.add(adjs.get(i).value);
			}
		}
		// 根据名词短语查找情感词
		List<NounPhrase> feaInSen = new ArrayList<>();
		HashSet<String> feaStrInSen = new HashSet<String>();
		for (int i = 0; i < nounPhrases.size(); i++) {
			if (feaDic.contains(nounPhrases.get(i).value)) {
				feaInSen.add(nounPhrases.get(i));
				feaStrInSen.add(nounPhrases.get(i).value);
			}
		}
		if (type.equals(Constant.FEATURE)) {
			HashSet<String> newFeas = new HashSet<String>();
			int adjIndexNo = -1;
			int adjWordNo = -1;
			NounPhrase noun = null;
			for (int i = 0; i < sentiInSen.size(); i++) {
				adjIndexNo = sentiInSen.get(i).indexNo;
				adjWordNo = sentiInSen.get(i).no;
				for (int j = 0; j < nounPhrases.size(); j++) {
					noun = nounPhrases.get(j);
					if (!feaStrInSen.contains(noun.value)
							&& adjWordNo - noun.l_no > 0
							&& adjWordNo - noun.l_no <= 3) {
						newFeas.add(noun.value);
					}
				}
			}
			return newFeas;
		}
		if (type.equals(Constant.SENTIMENT)) {
			HashSet<String> newSentis = new HashSet<String>();
			Adj adj = new Adj();
			for (int i = 0; i < feaInSen.size(); i++) {
				wordNo = feaInSen.get(i).f_no;
				for (int j = 0; j < adjs.size(); j++) {
					adj = adjs.get(j);
					if (!sentiStrInSen.contains(adj.value)
							&& adj.no - wordNo > 0 && adj.no - wordNo <= 3) {
						newSentis.add(adj.value);
					}
				}
			}
			return newSentis;
		}
		return null;
	}

	public void run() throws IOException {
		Configuration conf = new Configuration();
		// 获取新的特征词
		conf.set("type", Constant.FEATURE);
		String inPath = Constant.HDFS_TRAINING_PATH + "/negative";
		String outPath = Constant.HDFS_NEW_FEA_PATH;
		count(conf, "feaUpdateJob", inPath, outPath);
		// 获取新的情感词
		conf.set("type", Constant.SENTIMENT);
		outPath = Constant.HDFS_NEW_SENTI_PATH;
		count(conf, "feaUpdateJob", inPath, outPath);
		// 合并新特征词到特征词典
		HDFSUtil.appendFile(conf, Constant.HDFS_NEW_FEA_PATH,
				Constant.HDFS_FEA_DIC_PATH);
		// 合并新特情感词到情感词典
		HDFSUtil.appendFile(conf, Constant.HDFS_NEW_SENTI_PATH,
				Constant.HDFS_SENTI_DIC_PATH);
	}

	public void count(Configuration conf, String name, String inPath,
			String outPath) {
		Path in = new Path(inPath);
		Path out = new Path(outPath);
		try {
			Job job = Job.getInstance(conf, name);
			job.setJarByClass(UpdateDictDriver.class);
			job.setMapperClass(UpdateDictMapper.class);
			job.setReducerClass(UpdateDictReducer.class);
			job.setInputFormatClass(TextInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
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
