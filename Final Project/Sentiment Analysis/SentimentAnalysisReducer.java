package assignment.sentimentAnalysis;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SentimentAnalysisReducer extends Reducer<Text,Text,Text,Text> {
static SentimentAnalysisDriver driverObject=new SentimentAnalysisDriver();
static Set<String> goodWords=driverObject.getGoodWords();
static Set<String> badWords=driverObject.getBadWords();
Text result=new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	 int count = 0;
	 int positiveCount=0;
	 int negativeCount=0;
         String[] csv;
         while (values.iterator().hasNext()) {
         	//remove all non whitespace and non characters from review body, split by spaces between words
	            csv = values.iterator().next().toString().replaceAll("[^\\p{L}\\p{Z}]","").split(" ");
             for (String word : csv) {
             	if (goodWords.contains(word)) {
             		positiveCount++;
             	}
             	if (badWords.contains(word)) {
             		negativeCount++;
             	}
             }
         }
         try {
         float sentimentScore=((positiveCount-negativeCount)/(positiveCount+negativeCount))*100;
         result.set(String.valueOf(sentimentScore));
         context.write(key, result);
         }
         catch(Exception e) {
        	 result.set(String.valueOf(0));
         }
        
}
	
}

