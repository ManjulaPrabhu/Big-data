package assignment.movieRatings;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RatingsDriver {
	 public static void main( String[] args ) throws Exception
	    {
	    	Job job=Job.getInstance();
			job.setJarByClass(RatingsDriver.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(RatingMapper.class);
			job.setReducerClass(RatingReducer.class);
			
			job.setNumReduceTasks(2);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
	    }
}
