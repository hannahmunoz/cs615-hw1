import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.LongWritable.Comparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// I used http://kamalnandan.com/hadoop/how-to-find-top-n-values-using-map-reduce/
// to figure out how to get the top N values

public class Driver extends Configured implements Tool{
	
	public int run (String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// check arguments
		if (args.length != 2 ) {
			System.err.println("Not enough arguments <reduced hashtags file> <output file>");
			return -1;
		}
		
		// set up Job
		Job job = new Job (getConf());
		job.setJarByClass(Driver.class);
		job.setJobName("Tweet Mapper");

		// Mappers and Reducers
		job.setMapperClass(topMap.class);
		job.setCombinerClass (topCombiner.class);
		job.setReducerClass(topReduce.class);
		job.setSortComparatorClass (LongWritable.DecreasingComparator.class);

		job.setInputFormatClass (KeyValueTextInputFormat.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(LongWritable.class);
	    	job.setMapOutputValueClass(Text.class);    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// set up input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// exit upon completion
		boolean status = job.waitForCompletion(true);
		if (status) {
			return 0;
		}else {
			return 1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run (new Configuration(), new Driver(),args);
		System.exit (exit);
	} 


}
