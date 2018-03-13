import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.lang.Integer;

import org.apache.hadoop.mapreduce.Mapper;

public class topMap extends Mapper<Text, Text, LongWritable, Text>{
		@Override
		public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
			int val = Integer.parseInt (value.toString());	
				
			context.write(new LongWritable (val), key);
		}
}
