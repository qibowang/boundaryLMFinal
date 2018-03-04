package cn.edu.blcu.nlp.middleSenProb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbReducer extends Reducer<Text, Text, Text, Text>{
	private final char SEPARATOR = '▲';
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String valueStr = "";
	private String ngram = "";
	private long rawcount = 0l;
	private String items[];
	
	private int wordsNum;
	private int gtmin = 0;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long rawCountSum = 0l;
		boolean flag=false;
		Configuration conf = context.getConfiguration();
		List<Text> list = new ArrayList<Text>();
		for(Text value:values){
			valueStr = value.toString();
			items = valueStr.split("\t");
			ngram = items[0];
			wordsNum=ngram.length();
			rawcount = Long.parseLong(items[2]);
			rawCountSum += rawcount;
			if (ngram.charAt(wordsNum/2) == SEPARATOR) {
				list.add(WritableUtils.clone(value, conf));
				flag=true;
			}
		}
		if(flag){
			for (Text value : list) {
				valueStr = value.toString();
				items = valueStr.split("\t");
				ngram = items[0];
				rawcount = Long.parseLong(items[1]);
				if (rawcount >= gtmin) {
					resKey.set(ngram);
					// log.info("ngram===>"+ngram);
					// log.info("speed porb--->"+rawcount+"\t"+rawCountSum);
					resValue.set(Math.log10((double) rawcount / rawCountSum) + "\t" + rawcount);
					context.write(resKey, resValue);
				}
			}
		}
	}
}
