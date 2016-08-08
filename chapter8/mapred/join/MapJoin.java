package MapJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 
 Table1
 011990-99999    SIHCCAJAVRI
 012650-99999    TYNSET-HANSMOEN


 Table2
 012650-99999    194903241200    111
 012650-99999    194903241800    78
 011990-99999    195005150700    0
 011990-99999    195005151200    22
 011990-99999    195005151800    -11
 * */

public class MapJoin {
	static class mapper extends Mapper<LongWritable, Text, Text, Text> {
		private Map<String, String> Table1Map = new HashMap<String, String>();

		// 将小表读到内存HashMap中
		protected void setup(Context context) throws IOException {
			URI[] paths = context.getCacheFiles();

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsr = fs.open(new Path(paths[0].toString()));
			// BufferedReader br = new BufferedReader(new FileReader(
			// paths[0].toString()));
			String line = null;
			try {
				while ((line = fsr.readLine().toString()) != null) {
					String[] vals = line.split("\\t");
					if (vals.length == 2) {
						Table1Map.put(vals[0], vals[1]);
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				fsr.close();
			}
		}

		// 对大表进行Map扫描
		protected void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			String[] vals = val.toString().split("\\t");
			if (vals.length == 3) {
				// 每条记录都用外键对HashMap get
				String Table1Vals = Table1Map.get(vals[0]);
				Table1Vals = (Table1Vals == null) ? "" : Table1Vals;
				context.write(new Text(vals[0]), new Text(Table1Vals + "\t"
						+ vals[1] + "\t" + vals[2]));
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Parameter number is wrong, please enter three parameters：<big table hdfs input> <small table local input> <hdfs output>");
			System.exit(-1);
		}

		Job job = new Job(conf, "MapJoin");

		job.setJarByClass(MapJoin.class);
		job.setMapperClass(mapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.addCacheFile((new Path(args[1]).toUri()));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
