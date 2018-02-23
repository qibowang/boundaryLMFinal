package cn.edu.blcu.nlp.middleInnerSenProb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;




public class ProbDriver {
	
	public static void main(String[] args) {
		String numeratorPath="";
		String denominatorPath="";
		String probPath="";
		int isLzo=0;
		int tasks=1;
		int gtmin=1;
		boolean parameterValid=false;
		int parameterNum = args.length;
		for(int i=0;i<parameterNum;i++){
			if(args[i].equals("-numerator")){
				numeratorPath=args[++i];
				System.out.println("numerator--->"+numeratorPath);
			}else if(args[i].equals("-denominator")){
				denominatorPath=args[++i];
				System.out.println("denominator--->"+denominatorPath);
			}else if(args[i].equals("-prob")){
				probPath=args[++i];
				System.out.println("prob--->"+probPath);
			}else if(args[i].equals("-isLzo")){
				isLzo=Integer.parseInt(args[++i]);
				System.out.println("isLzo--->"+isLzo);
			}else if(args[i].equals("-tasks")){
				tasks=Integer.parseInt(args[++i]);
				System.out.println("tasks--->"+tasks);
			}else if(args[i].equals("-gtmin")){
				gtmin=Integer.parseInt(args[++i]);
				System.out.println("gtmin--->"+gtmin);
			}else{
				System.out.println("there exists invalid parameters--->"+args[i]);
				parameterValid=true;
			}
		}
		if(parameterValid){
			System.out.println("parameters invalid!!!!");
			System.exit(1);
		}
		
		try {
			Configuration conf = new Configuration();
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class, CompressionCodec.class);
			conf.setInt("gtmin", gtmin);
			Job middleProbJob = Job.getInstance(conf,"middle inner sen prob job");
			
			middleProbJob.setJarByClass(ProbDriver.class);
			middleProbJob.setReducerClass(ProbReducer.class);
			middleProbJob.setSortComparatorClass(ProbComparator.class);
			middleProbJob.setNumReduceTasks(tasks);
			
			middleProbJob.setMapOutputKeyClass(Text.class);
			middleProbJob.setMapOutputValueClass(Text.class);
			middleProbJob.setOutputKeyClass(Text.class);
			middleProbJob.setOutputValueClass(Text.class);
			
			MultipleInputs.addInputPath(middleProbJob, new Path(numeratorPath), SequenceFileInputFormat.class,ProbMapperNumerator.class);
			MultipleInputs.addInputPath(middleProbJob, new Path(denominatorPath), SequenceFileInputFormat.class,ProbMapperDenominator.class);
			middleProbJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputDirRecursive(middleProbJob, true);
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(probPath);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(middleProbJob, outputPath);
			middleProbJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (isLzo == 0) {
				setLzo(middleProbJob);
			}

			if (middleProbJob.waitForCompletion(true)) {
				System.out.println(middleProbJob.getJobName()+" Job successed");
			} else {
				System.out.println(middleProbJob.getJobName()+" Job failed");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void setLzo(Job job) {
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
	}
}
