# Stock-market-analysis-Hadoop

This homework has 2 parts. Some components might seem to be trivial, but please start early, since the programming aspect might be time-intensive (debugging is not easy).
Part A is mandatory with 2 problems. Part B is optional if you want some additional exercises.

A) MapReduce on pseudo-distributed Hadoop installation/ Cloudera VM (10 points)

1) WordCount 2.0
Requirement
Follow the simple WordCount tutorial in the Hadoop Tutorial part. Modify your WordCount program to satisfy the following requirements. 
Rename your program to MyWordCount.
Counting frequencies for punctuations and stop words is generally not useful. Also, current code will consider same word differently with and without punctuation e.g. "baig" and "baig." will be considered two separate words.
Remove punctuations from the text
Also, remove stop words from the text. A list of stop words can be found here. 
The program accepts 4 positional arguments. Your program should have error checking mechanism for invalid values of arguments.
Words are defined as  separated by comma, space, period(.) and tab(\t), parentheses(), brackets[], curly braces({}) characters.
Argument descriptions:
1. The number of reduce tasks to be used (number of reducers).
2. [true | false ] Case-sensitiveness. if the argument is true, the program takes into consideration case sensitivity. If the argument is false, the program ignores case sensitivity.
3. The HDFS input paths in comma separated format. The program should be able to process multiple files or directories. The number of input paths can be large.
e.g. filename1, filename2, filename3 and etc.
4. The HDFS output path for your program. Notice that you should remove the output path after every execution of your program. MapReduce cannot start a job if there exists a precreated output directory.
Obtaining sample data (terminal commands)
$ wget http://bmidb.cs.stonybrook.edu/publicdata/big.txt
$ wget http://textfiles.com/programming/writprog.pro
$ hdfs dfs -mkdir /user/cloudera/input
$ hdfs dfs -put big.txt /user/cloudera/input/
$ hdfs dfs -put writprog.pro /user/cloudera/input/

Input Examples
An example query on 2 large text files
$ hadoop MyWordCount.jar MyWordCount 4 true /home/cloudera/input/big.txt,/home/cloudera/input/writprog.pro /user/cloudera/outputWordCount

hadoop MyWordCount.jar MyWordCount 2 false /home/cloudera/input/big.txt,/home/cloudera/input/writprog.pro /user/cloudera/outputWordCount

To view output:
(listing content of the directory)
$ hdfs dfs -ls /home/cloudera/outputWordCount
(print out the content to the terminal)
$ hdfs dfs -cat /home/cloudera/outputWordCount/part-*

Note:
Please check your output files and check for differences when parameters are changed.

For Hadoop MapReduce using Java API
Passing parameters/static variables to Mapper or Reducer task in Java
Due to the nature of independent containers for JVM (shared-nothing architecture): to add a custom configuration settings/values:
Add to the main method:
          Configuration conf = new Configuration();
      conf.set("yourpropertyname", "value");
      Job job = Job.getInstance(conf, ...)
Add the following function/method to your mapper and/or reducer class (another sibling method to the map and reduce function you implement)
      public void setup(Context context) {
         Configuration config = context.getConfiguration();
         your_static_var = config.get("yourpropertyname");
      }


For Hadoop streaming (Python, C, C++ and etc.)
To pass arguments to your processes, you will want to surround the mapper and reducer parameter with double quotes "" to indicate these are arguments to your map/reduce tasks, not to hadoop.
E.g. (The entire segment is one line of code)
hadoop jar /usr/jars/hadoop-streaming-2.6.0-cdh5.4.2.jar \
    -D mapred.reduce.tasks=4 \
    -input <input-path1>,<input-path2>
    -output <output-path>
    -mapper 'mymapper.py true'
    -reducer 'myreducer.py true'
That is you can pass the entire command line arguments into your program and only use the relevant parameters.
(Note: the location of the jar might be different depending on our version of VM image.)


Submission
Submit your MyWordCount.java (it should contain the map and reduce functions).
Or for other languages than Java (C, C++, Python and etc.)
Submit your program in your favorite language and the shell/command line script you use to execute your file


2) Stock Price Analysis
In this exercise we will analyze daily values of stock prices of major companies.

Input data
Download the stock price data: 
bmidb.cs.stonybrook.edu/publicdata/StockPrices.zip
(Data is obtained from QuanDL)
(The file is compressed. The size is approximately 400MB; the decompressed size is approximately 1600MB ).

Sample data (for development purpose):
bmidb.cs.stonybrook.edu/publicdata/samplestockdata.csv

(Execute wget http://bmidb.cs.stonybrook.edu/publicdata/samplestockdata.csv )

Unzip the data and store it in a location of your choice.

Your MapReduce program should meet the following requirements:
Name your program AnalyzeStock.
The program accepts 6 positional arguments.
The first two arguments are the start and end dates of our interval of interest, which is specified by user. The date can be in MM/DD/YYYY format. You do NOT have to do date validation or user input error check.
The third argument is either "avg", "min" or "max", which is an aggregation operator on the data obtained for the interval. 
The fourth argument is the name of the value field we want to aggregate. The argument value should be either: closing price (close), lowest price of the day(low), highest price of the day (high). See the specifications below for the name of the input fields.
The fifth argument should be the name or the input file in HDFS.
The sixth argument should be the HDFS path for the output.
The output should have the aggregate value for each stock (i.e. each ticker).
The output format should be tab or space-separated (your choice): ticker aggregated_value.  
Additional note and specifications:
The input format is comma separated. The list of the fields are: ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
The date format in the input file is: yyyy-mm-dd (e.g. 1999-11-18).
You do not have to check for validity of your date input argument.
You can set the number of reducers (reduce tasks to 4 or 6).
You can hard code the input path and output path. Otherwise, you can specify the 4th and 5th argument to be the input path and output path.
Sample arguments
$ hadoop jar AnalyzeStock.jar AnalyzeStock 10/11/1990 12/09/2015 avg low /user/cloudera/samplestockdata.csv /user/cloudera/stockoutput
The output should contain the average temperature between 10/11/1990 and 12/09/2015 for each ticker.

$ hadoop jar AnalyzeStock.jar AnalyzeStock 02/20/2006 10/09/2015 min close /user/cloudera/samplestockdata.csv /user/cloudera/stockoutput
The output should contain the minimum temperature between 02/20/2006 and 10/09/2015 for each ticker.

Sample output (to compare your results)
Sorted output (you can "cat" all files and pipe it to a system sort function that produces the combined result):
Output for running: hadoop jar AnalyzeStock.jar AnalyzeStock 10/11/1990 12/09/2013 avg low /user/cloudera/stockdatafull.csv /user/cloudera/stockoutput1
http://bmidb.cs.stonybrook.edu/publicdata/result10-11-1990to12-09-2013avglow

You can view the output using terminal (e.g. hdfs dfs -cat /user/cloudera/stockoutput1/* ) or HUE console.
Remove the output directory if you need to rerun:  hdfs dfs -rm -r /user/cloudera/stockoutput1

Output for running: hadoop jar AnalyzeStock.jar AnalyzeStock 01/01/1995 01/01/2000 avg low /user/cloudera/samplestockdata.csv /user/cloudera/stockoutput1
A       46.5
AA      64.1
AAN     16
AAON    8.3
AAPL    33.6

Output for running: hadoop jar AnalyzeStock.jar AnalyzeStock 01/01/1995 01/01/2000 max close /user/cloudera/samplestockdata.csv /user/cloudera/stockoutput1
A       79.2
AA      89.5
AAN     30.6
AAON    15.5
AAPL    117.8

Output for running: hadoop jar AnalyzeStock.jar AnalyzeStock 01/01/1990 12/31/1998 min low  /user/cloudera/samplestockdata.csv /user/cloudera/stockoutput1
AA      36.9
AAN     6.5
AAON    1.1
AAPL    12.8

Hint: to access separate field from your input:
In Java, parse your line into an array of Strings:
String[] tmp = value.toString().split(",");
So
tmp[0], tmp[1], tmp[2], and etc. will refer to the first field, second field, third field and etc.

In Python, parse your line into an array of strings:
tmp = line.strip().split(",")
So
tmp[0], tmp[1], tmp[2], and etc. will refer to the first field, second field, third field and etc.

Hint:
You will need to design key-value formats to support sorting and merging. For combining fields, do not use tab character. Tab is by default the separator between key and value in MapReduce (Text-Text key).

Submission
Submit your AnalyzeStock.java file.
Or for other languages than Java (C, C++, Python and etc.)
Submit your program in your favorite language and the shell/command line script you use to execute your file
Observe that for arguments, you will want to surround the mapper and reducer parameter with double quotes "" to indicate these are arguments to your map/reduce tasks, not to hadoop.

Note that you can run your Hadoop program on EMR the same way as described in the next part of the homework (which is optional).

Please contact Furqan if you have questions on MapReduce/AWS:
fbaig [@] cs.stonybrook.edu

Extra Credit (3 points): Optional part of homework 
 B) MapReduce on Amazon Elastic MapReduce (EMR) 
Next we will perform some analysis on a slightly larger data to explore the power of MapReduce.

ALWAYS TERMINATE YOUR CLUSTER (CHECK/TURN OFF TERMINATE PROTECTION AND USE TERMINATE CLUSTER AFTER STEPS)

Requirements:
Name your program AnalyzeStockAdvanced.
The program should accept 2 positional parameters.
The first argument is the input path on HDFS or Amazon S3.
The second argument is the output path on HDFS or Amazon S3.
The goal is to output the ticker (stock) with the highest fluctuation for every YEAR for which any data exists in the database, which is defined as the difference between the high and low prices in that year.
You do not have to sort the final output by date.
You approximately 20 reducer tasks (set the number of reduce tasks as 20.)
Output should have 5 fields and fields are tab or space-separated (your choice): Year Ticker  Low  High  Difference
Documentation on how to run a custom JAR using the GUI interface
http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-launch-custom-jar-cli.html
You can use the AWS Command Line Interface to run the JAR, but it will require you to read the documentation (not recommended due to the length and the difficulty of setting up security keys.)
You can view the screenshots of the steps to set up  a cluster below in the download section of this page.

Important Setup Instruction
In Management Console, select Storage -> Amazon S3.
First in Amazon S3, create  a bucket for yourself.
Create an input subdirectory and a program subdirectory in your bucket.
Upload your data (programs and input) into the corresponding folders.

Second, in Amazon EMR, use advanced option.
Select only Hadoop to be installed. Delete Hive, PIG and etc.

Recommended settings
1) Use 4-6 nodes to test your data.
2) Leave auto-terminate on.

Job Chaining:
1) In your main program, create another job object and configure input and output path correspondingly. In other words, the input path of the next job should be the output of the current job.

To add steps
For both JAR and streaming execution, treate Amazon S3 path with prefix s3://, as if the data comes the distributed file system (HDFS). S3 simulates HDFS to Hadoop EMR.

AWS input S3 location (used as input path)
s3://cse532/stockdata.csv 
You can also upload the files to your own customized S3 location, but that is not recommended.

AWS output S3 location:
Use a directory that doesn't exist on S3. Otherwise, your job will fail. If you would like to reuse the output directory, please delete the directory from S3 console.

It is highly recommended that you test your program locally on your VM before testing it on the bigger data set.

Submission
Submit your AnalyzeStockAdvanced.java file(s) and the file containing results in the specified format.

Hint:
Test your program locally (similar to the previous part of the homework.)
You might need multiple MapReduce jobs to accomplish the goal. You can chain them in a single Java program or shell script. The output of the first job should be the input of the second job.

Important:
After finishing using Amazon EMR and checking/download results, make sure you delete your temporary and output data on Amazon S3 and terminate all existing cluster jobs.

Note for uses of JAR files on EMR
The first argument for the JAR is always the name of the program, which should be "AnalyzeStockAdvanced" in this case.
Otherwise, your program will fail!
(i.e. hadoop jar AnalyzeStockAdvanced.jar AnalyzeStockAdvanced)
The argument to hadoop must be the name of the class/program you want to start.

Note 2:
Due to a minor bug in s3, you might have an issue when you hardcode the Amas3 output path to your program.
Attempt to pass the output as args[0] and call new Path(...) to add the output path to your job object.
