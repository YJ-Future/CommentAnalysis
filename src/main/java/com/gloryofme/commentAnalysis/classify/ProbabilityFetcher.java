package com.gloryofme.commentAnalysis.classify;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import com.gloryofme.commentAnalysis.tools.HDFSUtil;

/**
 * 获取先验概率
 */
public class ProbabilityFetcher {

	private ProbabilityFetcher() {
	}

	public static HashMap<String, Float> getProbability(Configuration conf,
			String filePath) throws IOException {
		
		String content = HDFSUtil.readFile(conf, filePath);
		String[] pairs = content.split("\\n");
		String[] items = null;
		String word = null;
		Float prob = null;
		HashMap<String, Float> res = new HashMap<>();
		for (String pair : pairs) {
			items = pair.split("\t");
			if (items.length != 2)
				continue;
			word = items[0];
			prob = Float.valueOf(items[1]);
			res.put(word, prob);
		}
		return res;
	}
}
