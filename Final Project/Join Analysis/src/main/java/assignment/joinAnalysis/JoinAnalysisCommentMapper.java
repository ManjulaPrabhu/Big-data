package assignment.joinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinAnalysisCommentMapper extends Mapper<Object, Text, Text, Text> {
	Text businessId;

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		try {
			businessId = new Text(fields[3]);
			Text outputValue = new Text("B" + value);
			context.write(businessId, outputValue);
		} catch (Exception e) {
			businessId = new Text("0");
		}

	}

}
