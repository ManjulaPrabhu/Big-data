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
         String[] csv;
         while (values.iterator().hasNext()) {
         	//remove all non whitespace and non characters from review body, split by spaces between words
	            csv = values.iterator().next().toString().replaceAll("[^\\p{L}\\p{Z}]","").split(" ");
             for (String word : csv) {
             	if (goodWords.contains(word)) {
             		count = count+1;
             	}
             	if (badWords.contains(word)) {
             		count = count-1;
             	}
             }
         }
         if (count > 0) {
        	result.set("Positive Sentiment");
         }
         else if (count < 0) {
         	result.set("Negative Sentiment");
         }
         else {
         	result.set("Neutral Sentiment");
         }
         context.write(key, result);
        
}
	
}

