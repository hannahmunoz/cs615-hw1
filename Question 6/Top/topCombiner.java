import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class topCombiner extends Reducer < LongWritable, Text, LongWritable, Text>{
	int count = 0;
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(count < 5) {
			for(Text value: values) {
				context.write(key,value);
				count++;
			}
		}
	}

}
