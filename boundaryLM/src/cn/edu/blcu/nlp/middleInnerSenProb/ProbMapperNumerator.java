package cn.edu.blcu.nlp.middleInnerSenProb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbMapperNumerator extends Mapper<Text, LongWritable, Text, Text> {
	private Text resKey = new Text();
	private Text resValue = new Text();
	private String SEP_STRING = "▲";
	private String ngram;
	private long rawcount = 0l;
	private int gtmin = 1;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		gtmin = context.getConfiguration().getInt("gtmin", gtmin);
	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		ngram = key.toString();
		rawcount = value.get();
		if (rawcount >= gtmin) {
			resKey.set(ngram.replace(SEP_STRING, ""));
			resValue.set(rawcount + "\t" + ngram);
			context.write(resKey, resValue);
		}
	}
}
