import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class userMap extends Mapper<Object, Text, Text, Text>{
		//key/value
		Text keyOut = new Text();
		Text valueOut = new Text();

		@Override
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\n");
			for (String x: line) {
				String [] temp = x.split("\t");
				keyOut.set(temp [0]);
				valueOut.set(temp [1]);
				context.write(keyOut, valueOut);
			}
	}
}
