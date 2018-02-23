package cn.edu.blcu.nlp.middleInnerSenNgramCountNumerator;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumeratorCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable resValue = new IntWritable();
	@Override
	protected void reduce(Text ngram, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable value:values){
			sum+=value.get();
		}
		resValue.set(sum);
		context.write(ngram, resValue);
	}
}
