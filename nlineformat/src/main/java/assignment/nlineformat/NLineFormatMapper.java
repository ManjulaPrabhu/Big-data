package assignment.nlineformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineFormatMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			String[] fields=value.toString().split(",");
			Text t=new Text(fields[0]);
		    context.write(t, new IntWritable(Integer.parseInt(fields[1].toString())));
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
		}
		}
		
	}

