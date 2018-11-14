package assignment.movieRatings;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		String[] values=value.toString().split("::");
		Text movieId=new Text(values[1]);
		FloatWritable rating=new FloatWritable(Float.parseFloat(values[2]));
		context.write(movieId, rating);
		
	}
	

}
