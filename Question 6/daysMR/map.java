import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class map extends Mapper<Object, Text, Text, LongWritable>{
		//key/value
		Text wordOut = new Text();
		LongWritable one = new LongWritable (1);

		@Override
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			// current line
			String[] line = value.toString().split("\n");

			//locate day
			for (String x: line) {
				if (x.length() > 37) {
					String temp = x.substring(x.length()-19, x.length()-9);
					
					if (temp.startsWith("200") && (temp.endsWith("1") || temp.endsWith("2") ||
					    temp.endsWith("3") || temp.endsWith("4") || temp.endsWith("5") || temp.endsWith("6") ||
					    temp.endsWith("7") || temp.endsWith("8") || temp.endsWith("9") || temp.endsWith("0") )) {
						wordOut.set(temp);

						context.write(wordOut, one);
					}
				}
			}
		}
}
