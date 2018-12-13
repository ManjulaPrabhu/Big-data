package assignment.sentimentAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SentimentAnalysisDriver {
	static Set<String> goodWords=new HashSet<String>();
	static Set<String> badWords=new HashSet<String>();
	public static void main(String[] args) throws Exception{
		Job job=Job.getInstance();
		job.setJarByClass(SentimentAnalysisDriver.class);
		collectPositiveWords(args[2]);
		collectNegativeWords(args[3]);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(SentimentAnalysisMapper.class);
		job.setReducerClass(SentimentAnalysisReducer.class);
		
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		}
static void collectPositiveWords(String filePath) {
	try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(filePath)));
			String word;
			while ((word = fis.readLine()) != null) {
				goodWords.add(word);
			}
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception..File not found");
			}
}

static void collectNegativeWords(String filePath) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(filePath)));
			String word;
			while ((word = fis.readLine()) != null) {
				badWords.add(word);
			}
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception..File not found");
		}
}
Set<String>  getGoodWords(){
	return goodWords;
}
Set<String>  getBadWords(){
	return badWords;
}
}
