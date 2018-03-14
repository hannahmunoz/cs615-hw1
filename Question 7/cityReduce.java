import java.io.IOException;
import java.lang.Integer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class cityReduce extends Reducer <Text, LongWritable, Text, LongWritable>{
	
	public void reduce(Text key, Iterable<LongWritable> value, Context context)  throws IOException, InterruptedException  {
		int count = 0;
			// count all the occurances of the tweet
			while (value.iterator().hasNext()) {
				count += value.iterator().next().get();
			}
				
			// write it to a file
			LongWritable output = new  LongWritable (count);
			context.write(key, output);
	}	
}
