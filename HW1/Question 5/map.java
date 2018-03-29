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
			StringTokenizer st = new StringTokenizer(value.toString());
	
			// loop through file until all words have been counted and found
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				//locate hashtag
				if (token.startsWith("#")) {
					//split in case of mutliple tags
					String[] tokenList = token.split("#");
					for (String i : tokenList) {
						wordOut.set(i);
						context.write(wordOut, one);
					}
				}
			}
		}
}
