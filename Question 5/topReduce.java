import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class topReduce extends Reducer < LongWritable, Text, Text, LongWritable>{
	int count = 0;
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(count < 100) {
			for(Text value: values) {
				context.write(value, key);
				count++;
			}
		}
	}
}
