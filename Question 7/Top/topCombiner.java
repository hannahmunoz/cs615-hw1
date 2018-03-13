import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class topCombiner extends Reducer < LongWritable, Text, LongWritable, Text>{
	int mCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mCount = 0;
	}
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) {
		if(mCount < 5) {
			try {
				for(Text value: values) {
					context.write(key, value);
					mCount++;
				}
			} catch(Exception e) {
				
			}
		}
	}

}
