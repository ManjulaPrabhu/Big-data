package assignment.businessRating;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessRatingReducer extends Reducer<Text, CustomWritable, Text, CustomWritable> {

	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values,
			Reducer<Text, CustomWritable, Text, CustomWritable>.Context context)
			throws IOException, InterruptedException {

		CustomWritable outputObject = new CustomWritable();
		for (CustomWritable val : values) {
			float checkValue = val.getRating();
			String checkCategory = val.getBusinessCategory().toString().toLowerCase();
			if (checkValue > 4.5 && checkCategory.toLowerCase().contains("desserts")
					&& checkCategory.toLowerCase().contains("vegan")) {
				outputObject.setBusinessName(val.getBusinessName());
				outputObject.setRating(val.getRating());
				outputObject.setBusinessCategory(val.getBusinessCategory());
				context.write(key, outputObject);

			}
		}

	}
}
