package cn.edu.blcu.nlp.middleInnerSenProbSort;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InnerSenSortSortReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text value:values){
			context.write(key, value);
		}
	}
}
