package assignment.joinAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinAnalysisReducer extends Reducer<Text, Text, Text, Text> {
	private List<Text> listA = new ArrayList<Text>();
	private List<Text> listB = new ArrayList<Text>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		listA.clear();
		listB.clear();
		for (Text val : values) {
			if (val.charAt(0) == 'A') {
				listA.add(new Text(val.toString().substring(1)));

			} else if (val.charAt(0) == 'B') {
				listB.add(new Text(val.toString().substring(1)));
			}
		}
		// performing inner join
		if (!listA.isEmpty() && !listB.isEmpty()) {
			for (Text A : listA) {
				for (Text B : listB) {
					String[] businessFields = A.toString().split(",");
					String businessId = businessFields[0];
					String businessName = businessFields[1].substring(2, businessFields[1].length() - 2);
					String businessCity = businessFields[4];
					String businessRating = businessFields[9];
					String[] commentFields = B.toString().split("\\|");
					String commentGiven = commentFields[0];
					context.write(new Text(businessId + "," + businessName + "," + businessCity + "," + businessRating),
							new Text(commentGiven));
				}

			}

		}

	}
}
