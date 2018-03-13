import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class cityMap extends Mapper<Object, Text, Text, Text>{
		//key/value
		Text keyOut = new Text();
		Text valueOut = new Text();

		@Override
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		// current line
			String[] line = value.toString().split("\n");

			//locate city
			for (String x: line) {
				String [] temp = x.split("\t|,");
				keyOut.set(temp [1]);
				valueOut.set(temp [0]);
				context.write(keyOut, valueOut);
			}
		}
}
