package com.gloryofme.commentAnalysis;

/**
 * 常量信息
 */
public class Constant {
    private Constant() {
    }

    // HDFS路径前缀
    public static String HDFS_PREFIX = "hdfs://master:9000";

    // 特征词典的HDFS路径
    public static String HDFS_FEA_DIC_PATH = HDFS_PREFIX
            + "/comment/dic/fea/featureDictionary.txt";
    // 情感词典的HDFS路径
    public static String HDFS_SENTI_DIC_PATH = HDFS_PREFIX
            + "/comment/dic/sentiment/sentimentDictionary.txt";
    // 情感词类型
    public static String SENTIMENT = "SENTIMENT";
    // 特征词类型
    public static String FEATURE = "FEATURE";

    // Job 用到的HDFS数据地址

    //训练集路径
    public static String HDFS_TRAINING_PATH = HDFS_PREFIX + "/comment/training/raw";

    //训练集分词输出路径
    public static String HDFS_TRAINING_SEG_PATH = HDFS_PREFIX + "/comment/training/seg";

    // 新产生特征词路径
    public static String HDFS_NEW_FEA_PATH = HDFS_PREFIX
            + "/comment/dict/new_fea";

    // 新产生情感词路径
    public static String HDFS_NEW_SENTI_PATH = HDFS_PREFIX
            + "/comment/dict/new_senti";

    //停用词
    public static String STOP_WORDS = "刚", "的","我们","快递","不是","购物","呢",
            "要","自己","之","将","“","”","，","。","（","）","后","应",
            "到","某","后","个","是","位","一","两","在","中","或","有",
            "更","?","？","!","！","、",";","*","","这","就","了","说",
            "和","京东","时候","把","联通","什么","东西","我","你","它","就是",
            "着","起来";

}
