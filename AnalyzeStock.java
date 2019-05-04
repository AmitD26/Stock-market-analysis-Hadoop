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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStock {

  public static class StockAnalyzerMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private static FloatWritable mappedValue = null;
    private Text ticker = new Text();
    private String startDate, endDate, fieldName;
    
    @Override
    protected void setup(Mapper<Object, Text, Text, FloatWritable>.Context context)
    		throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	this.fieldName = conf.get("field_name");
    	this.startDate = conf.get("start_date");
    	this.endDate = conf.get("end_date");
    	super.setup(context);
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String inputString = value.toString();
        String[] inputRecord = inputString.split(",");
        int columnIndex = 0;
        if (fieldName.equals("close")) {
        	columnIndex = 5;
        }
        if (fieldName.equals("low")) {
        	columnIndex = 4;
        }
        if (fieldName.equals("high")) {
        	columnIndex = 3;
        }
        SimpleDateFormat csvDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat userInputDateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date start = null, end = null, current = null;
        try {
			start = userInputDateFormat.parse(startDate);
			end = userInputDateFormat.parse(endDate);
			current = csvDateFormat.parse(inputRecord[1]);
		} catch (ParseException e) {
			e.printStackTrace();
		}

        if ((current.after(start) && current.before(end)) || current.equals(start) || current.equals(end)) {
	        ticker.set(inputRecord[0]);
	        mappedValue = new FloatWritable(Float.parseFloat(inputRecord[columnIndex]));
	        context.write(ticker, mappedValue);
        }

    }
  }

  public static class AggregatorReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();
    private String aggregationOperator;

    @Override
    protected void setup(
    		Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
    		throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	this.aggregationOperator = conf.get("aggregation_operator");
    	super.setup(context);
    }
    
    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      if (aggregationOperator == "avg") {
      	  float sum = 0.0f;
      	  int count = 0;
      	  for (FloatWritable val : values) {
  	        sum += val.get();
  	        count++;
  	      }
      	  sum /= count;
      	  result.set(sum);
      	  context.write(key, result);
      }
      if (aggregationOperator == "min") {
    	  float min = Float.MAX_VALUE;
    	  for (FloatWritable val : values) {
	        min = Math.min(min, val.get());
	      }
    	  result.set(min);
    	  context.write(key, result);
      }
      if (aggregationOperator == "max") {
    	  float max = Float.MIN_VALUE;
    	  for (FloatWritable val : values) {
	        max = Math.max(max, val.get());
	      }
    	  result.set(max);
    	  context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {	  
    Configuration conf = new Configuration();
    
    if (args.length < 6) {
    	System.out.println("Insufficient arguments.");
    	System.exit(1);
    }
    if (args.length > 6) {
    	System.out.println("Too many arguments.");
    	System.exit(1);
    }
    if (!(args[2].equals("min") || args[2].equals("max") || args[2].equals("avg"))) {
    	System.out.println("Invalid arguments.");
    	System.exit(1);
    }
    if (!(args[3].equals("close") || args[3].equals("low") || args[3].equals("high"))) {
    	System.out.println("Invalid arguments.");
    	System.exit(1);
    }
    
    
    conf.set("start_date", args[0]);
    conf.set("end_date", args[1]);
    conf.set("aggregation_operator", args[2]);
    conf.set("field_name", args[3]);
    
    Job job = Job.getInstance(conf, "analyze stock");
    job.setJarByClass(AnalyzeStock.class);
    job.setMapperClass(StockAnalyzerMapper.class);
    job.setCombinerClass(AggregatorReducer.class);
    job.setReducerClass(AggregatorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    
    job.setNumReduceTasks(1);
    
    FileSystem hdfs = FileSystem.get(conf);
    String inputPath = args[4];
    if (hdfs.exists(new Path(inputPath))) {
		FileInputFormat.addInputPath(job, new Path(inputPath));
	}
	else {
		System.out.println(inputPath + " doesn't exist.");
		System.exit(1);
	}

    FileOutputFormat.setOutputPath(job, new Path(args[5]));
    
    if (hdfs.exists(new Path(args[5])))
        hdfs.delete(new Path(args[5]), true);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
