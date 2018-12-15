package assignment.joinAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinAnalysisBusinessMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		try {
		//Filtering out businesses that are in Las Vegas and that have rating more than 4.5
		if(fields[4].equals("Las Vegas") && Float.parseFloat(fields[9])>4.5) {
		Text businessId = new Text(fields[0]);
		Text outputValue = new Text("A" + value);
		context.write(businessId, outputValue);
		}
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

}
