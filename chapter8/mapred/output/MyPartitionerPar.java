package mapred.output;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
	 * partitioner的输入就是Map的输出
	 * @author Administrator
	 */
	public  class MyPartitionerPar  extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			int result = 0;
			if (key.equals("three")) {
				result = 0 % numPartitions;//0%3=0
			} else if (key.equals("four")) {
				result = 1 % numPartitions;  //1%3=1
			} else if (key.equals("right")) {
				result = 2 % numPartitions;  //2%3 =2
			}
			return result;
		}
	}
