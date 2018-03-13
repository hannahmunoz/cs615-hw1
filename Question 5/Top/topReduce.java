import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class topReduce extends Reducer < LongWritable, Text, Text, LongWritable>{
	int mCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mCount = 0;
	}
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) {
		if(mCount < 100) {
			try {
				for(Text value: values) {
					context.write(value, key);
					mCount++;
                                        if(mCount > 100) {
                                             break;
                                        }
				}
			} catch(Exception e) {
			}
		}
	}
}
