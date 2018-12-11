package assignment.joinAnalysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinAnalysisDriver {
	public static void main(String[] args) throws Exception {
		Job job=Job.getInstance();
		job.setJarByClass(JoinAnalysisDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		 job.setReducerClass(JoinAnalysisReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		  
		 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, JoinAnalysisUserMapper.class);
		 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, JoinAnalysisCommentMapper.class);
		 Path outputPath = new Path(args[2]);
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
		 }

