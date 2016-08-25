package ICTCLAS.I3S.AC;

import java.io.File;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Scanner;

import ICTCLAS.I3S.AC.ICTCLAS50;

/**
 * 分词
 */
public class Segmenter {
	public ICTCLAS50 testICTCLAS50;
	public static String mark_noun = "/n";
	public static String mark_adj = "/a";
	public static String mark_verb = "/v";
	//加载分词库以及导入用户词典
	public Segmenter(){
		testICTCLAS50 = new ICTCLAS50();
		// 分词所需库的路径
		String argu = ".";
		argu = new File("").getAbsolutePath()+"\\";  
		// 初始化
		try {
			if (testICTCLAS50.ICTCLAS_Init(argu.getBytes("GB2312")) == false) {
				System.out.println("Init Fail!");
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int nCount = 0;
		String usrdir = "userdict.txt"; // 用户字典路径
		usrdir.toCharArray();
		byte[] usrdirb = usrdir.getBytes();
		nCount = testICTCLAS50.ICTCLAS_ImportUserDictFile(usrdirb,0);
		System.out.println("导入的用户词数为" + nCount);
		nCount = 0;
	}
	
	// 获取特定词性的词语，以用空格分隔的String作为输出
	public String getWords(String sInput) throws Exception {
		byte s[] = sInput.getBytes("gbk");
		String Line = testICTCLAS_ParagraphProcess(s);
		return Line;
	}
//这个是要用到的函数，输入一个字符串，然后输出以空格为分隔的分词后的结果
	public String sentenceSeg(String sInput) throws Exception{
		byte s[] = sInput.getBytes("gbk");
		byte nativeBytes[] = testICTCLAS50.ICTCLAS_ParagraphProcess(s, 0,0);
		String nativeStr = new String(nativeBytes, 0, nativeBytes.length,"GB2312");	
		return nativeStr;
	}

	// ----------------------------我是分割线-----------------------------------------

	
	public String testICTCLAS_ParagraphProcess(byte[] sInput) throws Exception {
		byte nativeBytes[] = testICTCLAS50.ICTCLAS_ParagraphProcess(sInput, 0,2);
		String nativeStr = new String(nativeBytes, 0, nativeBytes.length,"GB2312");	
		return (nativeStr);
	}

	public String getWords(String sInput, String parts) throws Exception {
		String WordsSet = "";
		byte s[] = sInput.getBytes("gbk");
		String Line = testICTCLAS_ParagraphProcess(s);
		//System.out.println(Line);
		Scanner sin = new Scanner(new StringReader(Line));
		while (sin.hasNext()) {
			String str = sin.next();
			int pos = str.lastIndexOf('/');
			if (pos == -1)
				continue;
			int len = str.length();
			String words = str.substring(0, pos);
			String mark = str.substring(pos, len);
			//if (parts.equals(mark))
			if (mark.contains(parts))
				WordsSet = WordsSet + words + ' ';
		}
		return WordsSet;
	}
	
	public static void main(String[] args) throws Exception {
		String str = "哦哦陌陌摸摸哦哦 ";
		//String str = "好好好手机好好好好玩，我是形容词，手机我买了。手机质量外观，很棒屏幕很好，屏幕的分辨率很高的名词十一个不错。";
		ArrayList<String> strArray = new ArrayList<>();
		Segmenter analyzeit = new Segmenter();
		analyzeit.getWords("abcd");
//		strArray.add("不错");
//		strArray.add("好评");
//		strArray.add("不错");
//		strArray.add("问题");
//		strArray.add("顺畅");
//		strArray.add("漂亮");
//		strArray.add("快");
//		strArray.add("高");
//		strArray.add("完美");
//		strArray.add("精致");
//		strArray.add("满意");
		/*Segmenter analyzeit = new Segmenter();//这个是初始化的部分
		System.out.println(analyzeit.getWords(str));
		System.out.println(ProcessTestFile.ProcessTest(str, analyzeit));*/
//		System.out.println("###"+analyzeit.getWords(str));
//		//String x = analyzeit.getWords(S, Segmenter.mark_noun);
//		SentenseInfo s=new SentenseInfo();
//		s.getClause(analyzeit.getWords(str), strArray);
//		System.out.println(s.nounMap);
//		System.out.println(s.nounStr);
//		System.out.println(s.NAPair);
//		System.out.println(s.clauseArray);
	}
}
