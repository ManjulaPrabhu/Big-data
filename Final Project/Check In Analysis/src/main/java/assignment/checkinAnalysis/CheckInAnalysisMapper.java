package assignment.checkinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CheckInAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		if (key.get() == 0) {
			return;
		} else {
			Text businessId = new Text(fields[0]);
			Text dayCheckInCountComposite = new Text(fields[1] + ":" + fields[3]);
			context.write(businessId, dayCheckInCountComposite);
		}
	}
}
