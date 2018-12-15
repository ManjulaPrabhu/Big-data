package assignment.businessRating;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessRatingMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {
	CustomWritable writableObject = new CustomWritable();
	float valueIfInvalid = 0;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");

		if (key.get() == 0) {
			return;
		} else {
			Text businessID = new Text(fields[0]);

			try {
				writableObject.setBusinessName(fields[1].substring(2,fields[1].length()-3));
				writableObject.setRating(Float.parseFloat(fields[9]));
				writableObject.setBusinessState(fields[5]);
				writableObject.setPostalCode(fields[6]);
			}

			catch (NumberFormatException e) {
				writableObject.setRating(valueIfInvalid);
			}
			writableObject.setBusinessCategory(fields[12]);
			context.write(businessID, writableObject);
		}
	}
}
