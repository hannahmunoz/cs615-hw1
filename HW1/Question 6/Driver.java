import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.Comparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{

	private static final String outFile = "outputFile";

	public int run (String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// check arguments
		if (args.length != 2 ) {
			System.err.println("Not enough arguements <tweets file> <output file>");
			return -1;
		}
		
		// set up Job
		Job job = new Job (getConf());
		job.setJarByClass(Driver.class);
		job.setJobName("Day Mapper");

		// Mappers and Reducers
		job.setMapperClass(map.class);
		job.setReducerClass(reduce.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
	    	job.setMapOutputValueClass(LongWritable.class);    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// set up input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(outFile));
		
		// exit upon completion
		boolean status = job.waitForCompletion(true);
		if (status) {
			// set up Job
			Job job2 = new Job (getConf());
			job2.setJarByClass(Driver.class);
			job2.setJobName("Tweet Mapper");

			// Mappers and Reducers
			job2.setMapperClass(topMap.class);
			job2.setCombinerClass (topCombiner.class);
			job2.setReducerClass(topReduce.class);
			job2.setSortComparatorClass (LongWritable.DecreasingComparator.class);

			job2.setInputFormatClass (KeyValueTextInputFormat.class);
		
			job2.setNumReduceTasks(1);

			job2.setMapOutputKeyClass(LongWritable.class);
	    		job2.setMapOutputValueClass(Text.class);    
	    
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);
		
			// set up input and output files
			FileInputFormat.addInputPath(job2, new Path(outFile));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
			// exit upon completion
			boolean status2 = job2.waitForCompletion(true);
			if (status) {
				return 0;
			}else {
				return 1;
			}
		}else {
			return 1;
		}
	}

	
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run (new Configuration(), new Driver(),args);
		System.exit (exit);
	} 


}
