package mapred.output;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public   class FindMaxValueReducer extends Reducer<IntWritable, FloatWritable, Text, FloatWritable> {
		 public void reduce(IntWritable key, Iterable<FloatWritable> values, 
                 Context context
                 ) throws IOException, InterruptedException {
			 Iterator it = values.iterator();
			 float maxfloat=0, tmp;
			 if(it.hasNext())
				 maxfloat = ((FloatWritable)(it.next())).get();
			 else
			 {
				 context.write(new Text("Max float value: "), null);
				 return;
			 }
			 //System.out.print(maxfloat + " ");
			 while(it.hasNext())
			 {
				 tmp = ((FloatWritable)(it.next())).get();
				 //System.out.print(tmp + " ");
				 if (tmp > maxfloat) {
					 maxfloat = tmp;
				 }
			 }
			 context.write(new Text("Max float value: "), new FloatWritable(maxfloat));
		 }
}
