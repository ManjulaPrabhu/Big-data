package assignment.movieRatingMap;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable,Text,Text,SortedMapWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, SortedMapWritable>.Context context)
			throws IOException, InterruptedException {
		String[] values=value.toString().split("::");
		Text movieId=new Text(values[1]);
		SortedMapWritable ratingFrequencyMap=new SortedMapWritable();
		FloatWritable rating=new FloatWritable(Float.parseFloat(values[2]));
		ratingFrequencyMap.put(rating, new LongWritable(1));
		
		context.write(movieId, ratingFrequencyMap);
		
	}
}
