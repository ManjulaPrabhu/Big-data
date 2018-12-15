package assignment.businessRating;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessRatingReducer extends Reducer<Text, CustomWritable, NullWritable, CustomWritable> {

	@Override
	protected void reduce(Text key, Iterable<CustomWritable> values,
			Reducer<Text, CustomWritable, NullWritable, CustomWritable>.Context context)
			throws IOException, InterruptedException {

		CustomWritable outputObject = new CustomWritable();
		for (CustomWritable val : values) {
			float checkValue = val.getRating();
			String checkCategory = val.getBusinessCategory().toString().toLowerCase();
			if (checkValue > 3 && checkCategory.toLowerCase().contains("desserts")
					&& checkCategory.toLowerCase().contains("vegan")) {

				outputObject.setBusinessName(val.getBusinessName());
				outputObject.setRating(val.getRating());
				outputObject.setBusinessState(val.getBusinessState());
				outputObject.setPostalCode(val.getPostalCode());
				outputObject.setBusinessCategory(val.getBusinessCategory());
				context.write(NullWritable.get(), outputObject);

			}
		}

	}
}
