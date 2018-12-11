package assignment.joinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinAnalysisUserMapper extends Mapper<Object,Text,Text,Text>{
	
@Override
	protected void map(Object key, Text value, Mapper<Object, Text,Text,Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields=value.toString().split(",");
		
			Text userId=new Text(fields[0]);
			Text outputValue=new Text("A"+value);
			context.write(userId,outputValue);
		}
		
	}
	

