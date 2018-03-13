import java.io.IOException;
import java.lang.Integer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class map extends Mapper<Object, Text, Text, LongWritable>{
		//key/value
		Text keyOut = new Text();
		LongWritable valueOut = new LongWritable ();

		@Override
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		// current line
			String[] line = value.toString().split("\n");

			//locate city
			for (String x: line) {
				String [] temp = x.split("\t|,");
				if (temp.length == 3){
					keyOut.set(temp [1]);
					int val = Integer.parseInt(temp[2]);
					valueOut.set (val);
					context.write(keyOut, valueOut);
				}
			}
		}
}
