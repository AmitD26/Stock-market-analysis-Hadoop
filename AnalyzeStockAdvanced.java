import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStockAdvanced {

	public static class StockAnalyzerMapper
	extends Mapper<Object, Text, Text, DoubleWritable>{

		private static DoubleWritable mappedValue = null;
		private Text ticker = new Text();

		@Override
		protected void setup(Mapper<Object, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String inputString = value.toString();
			String[] inputRecord = inputString.split(",");

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date current = null;
			try {
				current = dateFormat.parse(inputRecord[1]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			@SuppressWarnings("deprecation")
			int year = current.getYear() + 1900;

			ticker.set(inputRecord[0] + " " + Integer.toString(year));
			
			mappedValue = new DoubleWritable(Double.parseDouble(inputRecord[3]));
			context.write(ticker, mappedValue);
			mappedValue = new DoubleWritable(Double.parseDouble(inputRecord[4]));
			context.write(ticker, mappedValue);

		}
	}

	public static class AggregatorReducer
	extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		protected void setup(
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
						throws IOException, InterruptedException {
			super.setup(context);
		}

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context
				) throws IOException, InterruptedException {

			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			String resultKey;
			for (DoubleWritable val : values) {
				min = Math.min(min, val.get());
				max = Math.max(max, val.get());
				System.out.println("Val");
				System.out.print(val.get());
				System.out.println("max");
				System.out.print(max);
				System.out.println("min");
				System.out.print(min);
			}
			
			double diff = max-min;

			result.set(diff);
			resultKey = key.toString() + " " + Double.toString(min) + " " + Double.toString(max) + " " + Double.toString(diff);
			System.out.println("Result");
			System.out.print(result.get());
			context.write(new Text(resultKey), result);
		}
	}

	public static class StockAnalyzerMapper1
	extends Mapper<Object, Text, IntWritable, Text>{

		private Text mappedValue = new Text();
		private IntWritable yearKey = new IntWritable();

		@Override
		protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String inputString = value.toString();
			String[] inputRecord = inputString.split("\t");

			String[] inputRecordSplit = inputRecord[0].split(" ");
			String tickerName = inputRecordSplit[0] + " " + inputRecordSplit[2] + " " + inputRecordSplit[3] + " " + inputRecordSplit[4];
			int year = Integer.parseInt(inputRecord[0].split(" ")[1]);
			String fluctuation = inputRecord[1];

			yearKey.set(year);
			mappedValue.set(tickerName + "," + fluctuation);
			context.write(yearKey, mappedValue);
		}
	}

	public static class AggregatorReducer1
	extends Reducer<IntWritable,Text,IntWritable,Text> {
		private Text ticker = new Text();
		private IntWritable yearKey = new IntWritable();

		@Override
		protected void setup(
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
						throws IOException, InterruptedException {
			super.setup(context);
		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			double max = Double.MIN_VALUE;
			String tickerName = "";
			double currentValue = 0.0;
			for (Text val : values) {

				if (val.toString().split(",").length == 2)
					currentValue = Double.parseDouble(val.toString().split(",")[1]);
				if (currentValue > max) {
					tickerName = val.toString().split(",")[0];
				}
			}
			yearKey = key;
			ticker.set(tickerName);
			context.write(yearKey, ticker);

		}

	}


	public static void main(String[] args) throws Exception {	  
		Configuration conf1 = new Configuration();

		if (args.length < 2) {
			System.out.println("Insufficient arguments.");
			System.exit(1);
		}
		if (args.length > 2) {
			System.out.println("Too many arguments.");
			System.exit(1);
		}

		Job job1 = Job.getInstance(conf1, "job 1");
		job1.setJarByClass(AnalyzeStockAdvanced.class);
		job1.setMapperClass(StockAnalyzerMapper.class);
//		job1.setCombinerClass(AggregatorReducer.class);
		job1.setReducerClass(AggregatorReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);

		job1.setNumReduceTasks(4);

		FileSystem hdfs = FileSystem.get(conf1);
		String inputPath = args[0];
		if (hdfs.exists(new Path(inputPath))) {
			FileInputFormat.addInputPath(job1, new Path(inputPath));
		}
		else {
			System.out.println(inputPath + " doesn't exist.");
			System.exit(1);
		}

		FileOutputFormat.setOutputPath(job1, new Path("intermediary file"));

		if (hdfs.exists(new Path("intermediary file")))
			hdfs.delete(new Path("intermediary file"), true);

		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "job 2");
		job2.setJarByClass(AnalyzeStockAdvanced.class);
		job2.setMapperClass(StockAnalyzerMapper1.class);
//		job2.setCombinerClass(AggregatorReducer1.class);
		job2.setReducerClass(AggregatorReducer1.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(4);

		String intermediaryFilePath = "intermediary file/part-r-00000";
		if (hdfs.exists(new Path(intermediaryFilePath))) {
			FileInputFormat.addInputPath(job2, new Path(intermediaryFilePath));
		}
		else {
			System.out.println(intermediaryFilePath + " doesn't exist.");
			System.exit(1);
		}

		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		if (hdfs.exists(new Path(args[1])))
			hdfs.delete(new Path(args[1]), true);

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
