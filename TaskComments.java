import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskComments {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private static IntWritable Length = new IntWritable();
    private Text word = new Text();
    String st = "Text=";	//to check start of comment
    String en = "CreationDate=";	//to check end of the comment
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	int len = 0;
		 int isFound;	//for finding the start of actual comment
	     int endFound;	//for finding the end of actual comment
        //boolean found = true;
        while (itr.hasMoreTokens()) {	//until there are no more tokens
            word.set(itr.nextToken());	//getting token in word
            isFound = (word.find(st));	//checking if actual comment starts from here
            
            while (isFound != -1) {	//if start of comment found	
        	  len = len + 1; //getting length of all the words in the comment but adding 1 for each word
        	  word.set(itr.nextToken());	//getting next token
         	  endFound = (word.find(en)); 	//check if end of comment found
         	 if (endFound != -1) {	//if found
        		  isFound = -1;	//turn loop condition false
        		 // found = false;	//and make found as false
          }
         	 //if (found == false)	//break the outer loop if end of comment found
            //	  break;
        }
            //send comment and length of comment to reducer
        
    }
        Length = new IntWritable(len);
        context.write(value, Length);
  }
  }
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  int sum = 0;
      int lines = 0;
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	lines = lines + 1; //keeping count of comments
    	
      Text avg = new Text("Average");
      //summing up all the values (lengths of comments)
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (lines == 320371) {
      int average = sum/(lines); //calculating average
      result = new IntWritable(average);
      context.write(avg, result);	//writing average
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task Comments");
    job.setJarByClass(TaskComments.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}