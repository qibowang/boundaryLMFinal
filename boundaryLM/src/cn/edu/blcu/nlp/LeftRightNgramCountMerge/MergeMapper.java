package cn.edu.blcu.nlp.LeftRightNgramCountMerge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
	private int startOrder = 8;
	private int endOrder = 8;
	private int wordsNum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		startOrder = conf.getInt("startOrder", startOrder);
		endOrder = conf.getInt("endOrder", endOrder);

	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		wordsNum = key.toString().length();
		if (wordsNum >= startOrder && wordsNum <= endOrder) {
			context.write(key, value);
		}
	}

}
