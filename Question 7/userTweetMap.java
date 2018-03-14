import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class userTweetMap extends Mapper<Object, Text, Text, LongWritable>{
		//key/value
		Text wordOut = new Text();
		LongWritable one = new LongWritable (1);

		@Override
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		// current line
			String[] line = value.toString().split("\n");
			//locate userID
			for (String x: line) {
				StringTokenizer token = new StringTokenizer(x);
				if (token.hasMoreTokens()) {
					String temp = token.nextToken();	
					if ( temp.startsWith ("1") || temp.startsWith("2") || temp.startsWith("3") || temp.startsWith("4") ||
					     temp.startsWith("5") || temp.startsWith("6") || temp.startsWith("7") || temp.startsWith("8") ||
					     temp.startsWith("9") || temp.startsWith("0") &&
						(temp.endsWith("1") || temp.endsWith("2") || temp.endsWith("3") || temp.endsWith("4") ||
						 temp.endsWith("5") || temp.endsWith("6") || temp.endsWith("7") || temp.endsWith("8") ||
						 temp.endsWith("9") || temp.endsWith("0")) && 
							(!temp.contains("-") || !temp.contains(":") || !temp.contains("/") || !temp.contains(",") 
							|| !temp.contains(".") || !temp.contains("%") || !temp.contains("&") )  ) {
								
							wordOut.set(temp);
							context.write(wordOut, one);
							}
				}
		 	}
		}
}
