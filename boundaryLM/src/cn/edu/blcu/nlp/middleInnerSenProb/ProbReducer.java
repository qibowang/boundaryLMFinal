package cn.edu.blcu.nlp.middleInnerSenProb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbReducer extends Reducer<Text,Text,Text,Text>{
	
	private String items[];
	private Text resKey = new Text();
	private Text resValue = new Text();
	private double prob=0.0;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String numeratorStr="";
		String denominatorStr="";
		String ngram="";
		for(Text value:values){
			items=value.toString().split("\t");
			if(items.length==2){
				ngram=items[1];
				numeratorStr=items[0];
			}else{
				denominatorStr=items[0];
			}
		}
		
		if(numeratorStr.length()!=0&&denominatorStr.length()!=0){
			resKey.set(ngram);
			prob=Math.log10((double)Long.parseLong(numeratorStr)/Long.parseLong(denominatorStr));
			resValue.set(String.valueOf(prob)+"\t"+numeratorStr);
			context.write(resKey, resValue);
		}
	}
}
