package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(WordCount.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);

		private final Text userid = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			// final StringTokenizer itr = new StringTokenizer(value.toString(),",");
			// int count = 1;
			final String ud;
			String data = value.toString();
			// System.out.println("original");
			// System.out.println(data);
			String[] datasplit = data.split(",");			// Splits every line on dilimiter ","
			final int check = Integer.parseInt(datasplit[1]); //converts into integer to check if the user id is divisible by 100
			if (check%100 == 0){
				userid.set(datasplit[1]);					
				context.write(userid,one);

			}

			

			//Code for including userids with zero followers
			// if(datasplit.length == 2){				// to distinguish between the files nodes.csv and edges.csv
			// 	ud = datasplit[1];
			// 	int check = Integer.parseInt(ud);		// checks if the userid is divisible by 100
			// 	if(check % 100 == 0) {
			// 		userid.set(ud);
			// 		context.write(userid, one);		
			// 				}
			// }
			// else{
			// 	ud = datasplit[0];
			// 	int check = Integer.parseInt(ud);
			// 	if(check % 100 == 0) {
			// 		userid.set(ud);
			// 		context.write(userid, zero);		// includes userids with zero followers
			//				}
			// }
			
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(WordCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new WordCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}