package assignment.customWritablestock;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockAnalysisReducer extends Reducer<Text, StockAnalysisWritable, Text, StockAnalysisWritable> {
	StockAnalysisWritable outputObject = new StockAnalysisWritable();

	@Override
	protected void reduce(Text key, Iterable<StockAnalysisWritable> values,
			Reducer<Text, StockAnalysisWritable, Text, StockAnalysisWritable>.Context context)
			throws IOException, InterruptedException {
		outputObject.setMaxAdjClosingPrice(0);
		outputObject.setMaxStockVolume(0);
		outputObject.setMinStockVolume(-1);
		
		for(StockAnalysisWritable val: values) {
			if(outputObject.getMaxAdjClosingPrice()==0 || val.getMaxAdjClosingPrice() > outputObject.getMaxAdjClosingPrice()) {
				outputObject.setMaxAdjClosingPrice(val.getMaxAdjClosingPrice());
			}
			
			if(outputObject.getMinStockVolume()<0){
				outputObject.setMinStockVolume(val.getMinStockVolume());
				outputObject.setMinStockVolumeDate(val.getMinStockVolumeDate());
			}else if(val.getMinStockVolume()<outputObject.getMinStockVolume()){
				outputObject.setMinStockVolume(val.getMinStockVolume());
				outputObject.setMinStockVolumeDate(val.getMinStockVolumeDate());
			}
			
			if(outputObject.getMaxStockVolume()==0 ) {
			
				outputObject.setMaxStockVolume(val.getMaxStockVolume());
				outputObject.setMaxStockVolumeDate(val.getMaxStockVolumeDate());
			}else if(val.getMaxStockVolume()>outputObject.getMaxStockVolume()) {
			
				outputObject.setMaxStockVolume(val.getMaxStockVolume());
				outputObject.setMaxStockVolumeDate(val.getMaxStockVolumeDate());
			}
	
		
		}
		context.write(key, outputObject);
		
	}

}


