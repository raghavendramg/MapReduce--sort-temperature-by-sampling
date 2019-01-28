import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.*;

import org.apache.hadoop.util.*;

public class ProbSamp extends Configured implements Tool{
	public static class ProbSMapClass extends Mapper<LongWritable,Text,DoubleWritable,Text> {
		//private Text outkey = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Parse the input string into a nice map
			try{
				String[] str = value.toString().trim().replaceAll(" +"," ").split(" ");
				String temperature=str[3];
				double temp_val= Double.parseDouble(temperature);
				context.write(new DoubleWritable(temp_val), new Text(temperature));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static class ProbSReduceClass extends Reducer<DoubleWritable, Text, DoubleWritable, NullWritable> {
	@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values)
			{
				//String [] str = val.toString().trim().replaceAll(" +"," ").split(" ");
				//String temperature=str[3];
				//double temp_val= Double.parseDouble(temperature);
				context.write(key, NullWritable.get());
			}
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		
		if (args.length != 3) {
			System.err.println("Usage: TotalOrderSorting <user data> <out> <sample rate>");
			System.exit(1);
		}
		
		Job job1 = new Job(conf, "sample");
		
		Path inputPath = new Path(args[0]);
		Path partitionFile = new Path(args[1] + "_partitions.lst");
		Path outputStage = new Path(args[1] + "_staging");
		Path outputOrder = new Path(args[1]);
		double sampleRate = Double.parseDouble(args[2]);
		
		FileSystem.get(new Configuration()).delete(outputOrder, true);
		FileSystem.get(new Configuration()).delete(outputStage, true);
		FileSystem.get(new Configuration()).delete(partitionFile, true);
				
		job1.setJarByClass(ProbSamp.class);
		
		job1.setMapperClass(ProbSMapClass.class);
		
		FileInputFormat.setInputPaths(job1, inputPath);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class); // Set the output format to a sequence file
		SequenceFileOutputFormat.setOutputPath(job1, outputStage);
		
		job1.setMapOutputKeyClass(DoubleWritable.class);
		job1.setMapOutputValueClass(Text.class);
      
		job1.setNumReduceTasks(0);
		
		job1.setOutputKeyClass(DoubleWritable.class);
		job1.setOutputValueClass(Text.class);
		
		int code = job1.waitForCompletion(true)? 0 : 1;
		//System.exit(code);
		//return 0;
		
		if (code == 0) {
			Job job2 = new Job(conf, "sample");
			
			job2.setJarByClass(ProbSamp.class);
			
			job2.setMapperClass(Mapper.class);
			job2.setReducerClass(ProbSReduceClass.class);
			
			job2.setNumReduceTasks(3);
			// Use Hadoop's TotalOrderPartitioner class
			job2.setPartitionerClass(TotalOrderPartitioner.class);
			
			TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(),partitionFile);
			
			job2.setOutputKeyClass(DoubleWritable.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(job2, outputStage);
			
			FileOutputFormat.setOutputPath(job2, outputOrder);
			// Use the InputSampler to go through the output of the previous
			// job, sample it, and create the partition file
			InputSampler.writePartitionFile(job2,new InputSampler.RandomSampler<DoubleWritable, Text>(sampleRate, 10000));
			
			code = job2.waitForCompletion(true) ? 0 : 2;
		}
		
		FileSystem.get(new Configuration()).delete(partitionFile, false);
		FileSystem.get(new Configuration()).delete(outputStage, true);
		System.exit(code);
		return 0;
	}
   
	public static void main(String ar[]) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new ProbSamp(),ar);
		System.exit(0);
	}
	

}