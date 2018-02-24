package cn.edu.blcu.nlp.LeftRightBack;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackMapper extends Mapper<Text, LongWritable, Text, Text> {
	
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String prefix;//前缀
	private String suffix;//后缀
	private final String SEP_STRING="▲";
	private final String CIRCULAR_STRING="●";
	
	private final char SEP_CHAR = '▲';
	private final char CIRCULAR_CHAR='●';
	private String ngram;
	private int wordsNum;

	private String lmFlag = "";

	private Logger log = LoggerFactory.getLogger(BackMapper.class);
	@Override
	protected void setup(Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		lmFlag = conf.get("lmFlag");
	}

	@Override
	protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ngram = key.toString();
		wordsNum = ngram.length();
		ngram=circularReplace(ngram, lmFlag, wordsNum);
		if (lmFlag.equalsIgnoreCase("right")) {
			if (wordsNum == 1) {
				resKey.set("unigram");
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			} else if (ngram.charAt(0) == SEP_CHAR) {
				prefix = ngram.substring(0, wordsNum - 1);
				resKey.set(prefix);
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		} else if (lmFlag.equalsIgnoreCase("left")) {
			if (wordsNum == 1) {
				resKey.set("unigram");
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			} else if (ngram.charAt(wordsNum-1) == SEP_CHAR) {
				suffix= ngram.substring(1);
				resKey.set(suffix);
				resValue.set(ngram + "\t" + value.get());
				context.write(resKey, resValue);
			}
		}else{
			log.info("lmFlag is not set or invalid pls check again");
			
		}
	}
	
	private String circularReplace(String ngram,String lmFlag,int wordsNum){
		if(lmFlag.equalsIgnoreCase("left")){
			if(ngram.charAt(wordsNum-1)==CIRCULAR_CHAR){
				ngram=ngram.substring(0,wordsNum-1).replaceAll(CIRCULAR_STRING, SEP_STRING);
				ngram=ngram+CIRCULAR_CHAR;
			}else{
				ngram = ngram.replaceAll(CIRCULAR_STRING, SEP_STRING);
			}
			
		}else if(lmFlag.equalsIgnoreCase("right")){
			if(ngram.charAt(0)==CIRCULAR_CHAR){
				ngram=ngram.substring(1).replaceAll(CIRCULAR_STRING, SEP_STRING);
				ngram=CIRCULAR_CHAR+ngram;
			}else{
				ngram = ngram.replaceAll(CIRCULAR_STRING, SEP_STRING);
			}
			
		}
		return ngram;
	}

}
