package mapred.output;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class FindMaxValueMapper extends
			Mapper<IntWritable, ArrayWritable, IntWritable, FloatWritable> {
		private final static IntWritable one = new IntWritable(1);
		// private PrintWriter file=null;
		public void map(IntWritable key, ArrayWritable value, Context context)
				throws IOException, InterruptedException {
			FloatWritable[] floatArray = (FloatWritable[]) value.toArray();
			float maxfloat = floatArray[0].get();
			float tmp;
			for (int i = 1; i < floatArray.length; i++) {
				tmp = floatArray[i].get();
				if (tmp > maxfloat) {
					maxfloat = tmp;
				}
			}
			context.write(one, new FloatWritable(maxfloat));
		}
	}
