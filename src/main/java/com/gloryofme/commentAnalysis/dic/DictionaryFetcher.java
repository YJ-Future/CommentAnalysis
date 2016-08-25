package com.gloryofme.commentAnalysis.dic;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;

import com.gloryofme.commentAnalysis.Constant;
import com.gloryofme.commentAnalysis.tools.HDFSUtil;
/**
 * 获取特征词典工具类
 * @author YU
 */
public class DictionaryFetcher {
	private DictionaryFetcher() {
	}
	
	/**
	 * 获取特征根词典
	 * @param conf
	 * @return
	 */
	public static HashSet<String> getFeaDic(Configuration conf) {
		HashSet<String> dic = new HashSet<>();
		try {
			dic = getDic(conf, Constant.HDFS_FEA_DIC_PATH);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dic;
	}
	
	/**
	 * 获取情感词典
	 * @param conf
	 * @return
	 */
	public static HashSet<String> getSentiDic(Configuration conf) {
		HashSet<String> dic = new HashSet<>();
		try {
			dic = getDic(conf,Constant.HDFS_SENTI_DIC_PATH);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dic;
	}
	
	/**
	 * 获取词典
	 * @param conf
	 * @param filePath 词典文件HDFS位置
	 * @return
	 * @throws IOException
	 */
	private static HashSet<String> getDic(Configuration conf, String filePath)
			throws IOException {
		String content = HDFSUtil.readFile(conf, filePath);
		String[] pairs = content.split("\\r\\n");
		String[] items = null;
		String fea = null;
		HashSet<String> dic = new HashSet<>();
		for (String pair : pairs) {
			items = pair.split(" ");
			fea = items[0];
			dic.add(fea);
		}
		return dic;
	}
}
