package mapred.join;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class MapSideJoin  extends Configured implements Tool{
	private final static String inputa = "hdfs://Ubuntu1:9000/user/hadoop/join/Customers.csv";
	private final static String inputb = "hdfs:// Ubuntu1:9000/user/hadoop/join/Orders.csv";
	private final static String output = "hdfs:// Ubuntu1:9000/user/hadoop/join/output";
	public static class MapClass extends MapReduceBase
							  implements Mapper<Text, Text, Text, Text> {
		private Hashtable<String, String> joinData = new Hashtable<String, String>();
		@Override
		public void configure(JobConf conf) {
			try {
				Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					String[] tokens;
					BufferedReader joinReader = new BufferedReader(
							                new FileReader(cacheFiles[0].toString()));
					try {
						while ((line = joinReader.readLine()) != null) {
							tokens = line.split(",", 2);
							joinData.put(tokens[0], tokens[1]);
						}
					}finally {
						joinReader.close();
					}}} catch (IOException e) {
						System.err.println("Exception reading DistributedCache: " + e);
					}
				}
		public void map(Text key, Text value,OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
	
			String joinValue = joinData.get(key.toString());
			if (joinValue != null) {
				output.collect(key,new Text(value.toString() + "," + joinValue));
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		DistributedCache.addCacheFile(new Path(inputa).toUri(), conf);
		JobConf job = new JobConf(conf, DataJoinDC.class);	
		Path in = new Path(inputb);
		Path out = new Path(output);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setJobName("DataJoin with DistributedCache");
		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.set("key.value.separator.in.input.line", ",");
		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new DataJoinDC(), args);
		System.exit(res);
	}

}
