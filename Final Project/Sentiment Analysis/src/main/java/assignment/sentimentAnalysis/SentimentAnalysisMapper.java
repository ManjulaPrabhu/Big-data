package assignment.sentimentAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		Text businessID;
		if (key.get() == 0) {
			return;
		} else {
			try {
				businessID = new Text(fields[3]);
			} catch (Exception e) {
				businessID = new Text("");
			}
			Text commentGiven = new Text(fields[0]);
			context.write(businessID, commentGiven);
		}
	}
}