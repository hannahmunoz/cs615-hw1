import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class reduce extends Reducer <Text, Text, Text, Text>{
	Text valEmit = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException  {
            String number = "";
	    String character = "";
        for (Text value : values) {
            // ordering output
            String val = value.toString();

            char myChar = val.charAt(0);

            if (Character.isDigit(myChar)) {
                number = val;
            } else{
		character = val;
		}
        }

        valEmit.set(character + " " +number);
        context.write(key, valEmit);
    }

		/*int count = 0;
			// count all the occurances of the tweet
			while (value.iterator().hasNext()) {
				count++;
				value.iterator().next();
			}
				
			// write it to a file
			LongWritable output = new  LongWritable (count);
			context.write(key, output);*/
	//}	
}
