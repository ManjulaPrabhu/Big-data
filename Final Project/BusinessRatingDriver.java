package assignment.businessRating;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BusinessRatingDriver {
	public static void main(String[] args) throws Exception{
		Job job=Job.getInstance();
		job.setJarByClass(BusinessRatingDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(BusinessRatingMapper.class);
		job.setReducerClass(BusinessRatingReducer.class);
		
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CustomWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		}
}
