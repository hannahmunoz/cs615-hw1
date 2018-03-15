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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{
	private static final String tweetCountFile = "tweetCountFile";
	private static final String joinFile = "joinFile";
	private static final String countFile = "countFile";

	public int run (String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// check arguments
		if (args.length != 3 ) {
			System.err.println("Not enough arguements <tweets file> <user file> <output file>");
			return -1;
		}
		
		// set up Job
		Job job = new Job (getConf());
		job.setJarByClass(Driver.class);
		job.setJobName("User Tweets Mapper");

		// Mappers and Reducers
		job.setMapperClass(userTweetMap.class);
		job.setReducerClass(userTweetReduce.class);
		
		// 3 taks per hw requirements
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
	    	job.setMapOutputValueClass(LongWritable.class);    
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// set up input and output files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tweetCountFile));
		
		// exit upon completion
		boolean status = job.waitForCompletion(true);
		if (status) {
			// set up Job
			Job cityJob = new Job (getConf());
			cityJob.setJarByClass(Driver.class);
			cityJob.setJobName("City Mapper");

			cityJob.setReducerClass(joinReduce.class);

			cityJob.setNumReduceTasks(1);
			cityJob.setMapOutputKeyClass(Text.class);
	    		cityJob.setMapOutputValueClass(Text.class);    
	    
			cityJob.setOutputKeyClass(Text.class);
			cityJob.setOutputValueClass(LongWritable.class);
		
			// set up input and output files
			MultipleInputs.addInputPath(cityJob, new Path(args[1]), TextInputFormat.class, cityJoinMap.class);
			MultipleInputs.addInputPath(cityJob, new Path(tweetCountFile), TextInputFormat.class, userMap.class);
			FileOutputFormat.setOutputPath(cityJob, new Path(joinFile));
		
			// exit upon completion
			boolean cityStatus = cityJob.waitForCompletion(true);
			if (cityStatus) {
					// set up Job
					Job countJob = new Job (getConf());
					countJob.setJarByClass(Driver.class);
					countJob.setJobName("City Mapper");

					// Mappers and Reducers
					countJob.setMapperClass(cityMap.class);
					countJob.setReducerClass(cityReduce.class);
		
					countJob.setNumReduceTasks(1);
					countJob.setMapOutputKeyClass(Text.class);
	    				countJob.setMapOutputValueClass(LongWritable.class);    
	    
					countJob.setOutputKeyClass(Text.class);
					countJob.setOutputValueClass(LongWritable.class);
		
					// set up input and output files
					FileInputFormat.addInputPath(countJob, new Path(joinFile));
					FileOutputFormat.setOutputPath(countJob, new Path(countFile));
		
					// exit upon completion
					boolean countStatus = countJob.waitForCompletion(true);
					if (countStatus) {
						// set up Job
						Job topJob = new Job (getConf());
						topJob.setJarByClass(Driver.class);
						topJob.setJobName("Top City Mapper");

						// Mappers and Reducers
						topJob.setMapperClass(topMap.class);
						topJob.setCombinerClass (topCombiner.class);
						topJob.setReducerClass(topReduce.class);
						topJob.setSortComparatorClass (LongWritable.DecreasingComparator.class);

						topJob.setInputFormatClass (KeyValueTextInputFormat.class);
		
						topJob.setNumReduceTasks(1);

						topJob.setMapOutputKeyClass(LongWritable.class);
					    	topJob.setMapOutputValueClass(Text.class);    
					    
						topJob.setOutputKeyClass(Text.class);
						topJob.setOutputValueClass(LongWritable.class);
		
						// set up input and output files
						FileInputFormat.addInputPath(topJob, new Path(countFile));
						FileOutputFormat.setOutputPath(topJob, new Path(args[2]));
		
						// exit upon completion
						boolean jobStatus = topJob.waitForCompletion(true);
						if (jobStatus) {
							return 0;
						}else {
							return 1;
						}
					}else {
						return 1;
					}
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
