package cn.edu.blcu.nlp.middleSenProb;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbMapper extends Mapper<Text, LongWritable, Text, Text>{
	private int startOrder=7;
	private int endOrder=7;
	private int wordsNum;
	private String ngram;
	private String joinKey;
	private int orderTemp;
	private int midIndex;
	private Text resKey = new Text();
	private Text resValue = new Text();
	private final String SEP_STRING="▲";
	private final String CIRCULAR_STRING="●";
	
	private final char SEP_CHAR='▲'; 
	private final char CIRCULAR_CHAR='●';
	private String leftTemp="";
	private String rightTemp="";
	private char midChar;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf= context.getConfiguration();
		startOrder=conf.getInt("startOrder", startOrder);
		endOrder=conf.getInt("endOrder", endOrder);
	}
	@Override
	protected void map(Text key, LongWritable value, Context context)
			throws IOException, InterruptedException {
		ngram=key.toString();
		wordsNum=ngram.length();
		
		
		
		if(wordsNum%2==1){
			midIndex=wordsNum/2;
			midChar=ngram.charAt(midIndex);
			if(midChar==SEP_CHAR){
				leftTemp=ngram.substring(0,midIndex).replaceAll(CIRCULAR_STRING, SEP_STRING);
				rightTemp=ngram.substring(midIndex+1).replaceAll(CIRCULAR_STRING, SEP_STRING);
				ngram=leftTemp+SEP_STRING+rightTemp;
			}else{
				leftTemp=ngram.substring(0,midIndex).replaceAll(CIRCULAR_STRING, SEP_STRING);
				rightTemp=ngram.substring(midIndex+1).replaceAll(CIRCULAR_STRING, SEP_STRING);
				ngram=leftTemp+midChar+rightTemp;
			}
			for(orderTemp=startOrder;orderTemp<=endOrder;orderTemp++){
				if(orderTemp%2==1){
					if(wordsNum==orderTemp){
						
						joinKey=leftTemp+rightTemp;
						resKey.set(joinKey);
						resValue.set(ngram+"\t"+value.get());
						context.write(resKey, resValue);
					}
				}
			}
		}
	}
}
