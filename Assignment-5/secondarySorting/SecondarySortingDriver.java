package assignment.secondarySorting;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class SecondarySortingDriver {
	public static void main(String[] args) throws Exception{
		Job job=Job.getInstance();
		job.setJarByClass(SecondarySortingDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(SecondarySortingMapper.class);
		job.setReducerClass(SecondarySortingReducer.class);
		
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setSortComparatorClass(CompositeKeyComaparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		}
}
