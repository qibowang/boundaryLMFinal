package cn.edu.blcu.nlp.middleSenProb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbCombiner extends Reducer<Text, Text, Text, Text>{
	private final char SEPARATOR = '▲';
	//private Text resKey = new Text();
	private Text resValue = new Text();
	
	private String rawCount;
	private String ngram;
	private String items[];
	private int wordsNum;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long rawCountSum=0l;
		String valueStr="";
		for(Text value:values){
			valueStr=value.toString();
			items = valueStr.split("\t");
			
			ngram = items[0];
			wordsNum=ngram.length();
			
			//log.info("ngram--->"+ngram);
			rawCount = items[1];
			
			rawCountSum += Long.parseLong(rawCount);
			if(ngram.charAt(wordsNum/2)==SEPARATOR){
				resValue.set(valueStr+"\t"+rawCountSum);
				context.write(key, resValue);
				rawCountSum=0l;
			}
		}
		
		if(rawCountSum!=0){
			resValue.set(valueStr+"\t"+rawCountSum);
			context.write(key, resValue);
		}
	}
}
