package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordMain {
	 public static void main(String[] args) throws Exception {  
		    Configuration conf = new Configuration();  
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
		    /** 
		     * �������������/��� 
		     */  
		    if (otherArgs.length != 2) {  
		      System.err.println("Usage: wordcount <in> <out>");  
		      System.exit(2);  
		    }  
		    Job job = new Job(conf, "word count");  
		    job.setJarByClass(WordMain.class);//����  
		    job.setMapperClass(WordMapper.class);//mapper  
		    job.setCombinerClass(WordReducer.class);//��ҵ�ϳ���  
		    job.setReducerClass(WordReducer.class);//reducer  
		    job.setOutputKeyClass(Text.class);//������ҵ������ݵĹؼ���  
		    job.setOutputValueClass(IntWritable.class);//������ҵ���ֵ��  
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//�ļ�����  
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//�ļ����  
		    System.exit(job.waitForCompletion(true) ? 0 : 1);//�ȴ�����˳�  
		  }  
	}   
