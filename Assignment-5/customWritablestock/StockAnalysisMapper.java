package assignment.customWritablestock;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockAnalysisMapper  extends Mapper<LongWritable,Text,Text,StockAnalysisWritable>{
StockAnalysisWritable stockAnalysisObject=new StockAnalysisWritable();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, StockAnalysisWritable>.Context context)
			throws IOException, InterruptedException {
		
		String[] fields=value.toString().split(",");
		
		Text t=new Text(fields[0]);
		
		
		stockAnalysisObject.setMinStockVolume(Long.parseLong(fields[7]));
		stockAnalysisObject.setMaxStockVolume(Long.parseLong(fields[7]));
	
		
		stockAnalysisObject.setMaxAdjClosingPrice(new Float(fields[8]));
	
		stockAnalysisObject.setMinStockVolumeDate(fields[2]);
		stockAnalysisObject.setMinStockVolumeDate(fields[2]);
		
		
		context.write(t, stockAnalysisObject);
		
	}

}
