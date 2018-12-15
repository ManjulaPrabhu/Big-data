package assignment.sentimentAnalysis;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {
	static SentimentAnalysisDriver driverObject = new SentimentAnalysisDriver();
	static Set<String> goodWords = driverObject.getGoodWords();
	static Set<String> badWords = driverObject.getBadWords();
	Text result = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		float positiveReviewCount = 0;
		float negativeReviewCount = 0;
		int totalReviews = 0;
		String[] csv;
		while (values.iterator().hasNext()) {
			// remove all non whitespace and non characters from review body, split by
			// spaces between words
			int positiveCount = 0;
			int negativeCount = 0;
			csv = values.iterator().next().toString().replaceAll("[^\\p{L}\\p{Z}]", "").split(" ");
			for (String word : csv) {
				if (goodWords.contains(word)) {
					positiveCount++;
				}
				if (badWords.contains(word)) {
					negativeCount++;
				}
			}
			if (positiveCount >= negativeCount) {
				positiveReviewCount += 1;
			} else {
				negativeReviewCount += 1;
			}
			totalReviews += 1;

		}
		try {
			String sentimentScores = String.format(",%s	,%s", positiveReviewCount / totalReviews,
					negativeReviewCount / totalReviews);
			result.set(sentimentScores);
			if (totalReviews > 80) {
				context.write(key, result);
			}
		} catch (Exception e) {
			result.set(String.valueOf(0));
		}

	}

}
