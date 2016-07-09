package mapred.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxValueDriver {
	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] paths = { "hdfs://192.168.153.100:9000/user/hadoop/output3/" };
		String[] otherArgs = new GenericOptionsParser(conf, paths)
				.getRemainingArgs();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: FindMaxValue <OutputDir>");
			//System.exit(2);
		}
		conf.set("NumOfValues", "111");
		conf.set("hadoop.job.ugi", "jayliu,jayliu");
		conf.set("mapred.system.dir",
				"/home/hadoop/HadoopHome/tmp/hadoop-hadoop/mapred/system/");
		Job job = new Job(conf, "FindMaxValue");
		// job.setNumReduceTasks(2);
		job.setJarByClass(MaxValueDriver.class);
		job.setMapperClass(FindMaxValueMapper.class);
		job.setReducerClass(FindMaxValueReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setInputFormatClass(FindMaxValueInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		System.out.println(conf.get("mapred.job.tracker"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}   
