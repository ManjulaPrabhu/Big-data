package assignment.customWritablestock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class StockAnalysisDriver {
	public static void main( String[] args ) throws Exception
    {
    	Job job=Job.getInstance();
		job.setJarByClass(StockAnalysisDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(StockAnalysisMapper.class);
		job.setReducerClass(StockAnalysisReducer.class);
		
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StockAnalysisWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
    }
}
