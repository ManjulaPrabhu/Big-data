package assignment.joinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinAnalysisCommentMapper extends Mapper<Object,Text,Text,Text>{
	Text userId;
@Override
	protected void map(Object key, Text value, Mapper<Object, Text,Text,Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields=value.toString().split("|");
		try {	
		 userId=new Text(fields[4]);
		}
		catch(Exception e) {
			userId=new Text("0");
		}
		Text outputValue=new Text("B"+value);
			context.write(userId,outputValue);
		}

	}
	

