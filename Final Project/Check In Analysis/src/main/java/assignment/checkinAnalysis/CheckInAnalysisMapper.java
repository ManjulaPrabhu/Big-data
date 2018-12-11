package assignment.checkinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CheckInAnalysisMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text,IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] fields=value.toString().split(",");
		if(key.get()==0) {
			return;
		}else {
			Text dayOfTheWeek=new Text(fields[1]);
			IntWritable checkInCount=new IntWritable(Integer.parseInt(fields[3]));
			context.write(dayOfTheWeek,checkInCount);
		}
		}
	}
	

