package assignment.secondarySorting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<CompositeKeyWritable, IntWritable> {

	@Override
	public int getPartition(CompositeKeyWritable key, IntWritable value, int numReduceTasks) {

		return (key.getBusinessId().hashCode() % numReduceTasks);
	}
}
