import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

	// learned joins from https://stackoverflow.com/questions/27349743/hadoop-multiple-inputs
public class reduce extends Reducer <Text, Text, Text, Text>{
	Text valEmit = new Text();
	String merge = "";

	public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException  {
	String character = "";
	String number = "";
	for (Text value : values){
		String val = value.toString();
		char testChar = val.charAt (0);
	
		if (Character.isDigit (testChar)){
			number = val;
		}	
		else{
			character = val;
		}
	}

	merge = character +"\t"+ number;
	valEmit.set (merge);
	context.write (key, valEmit);
}
}
