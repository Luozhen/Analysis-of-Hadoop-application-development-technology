package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
	/** 
   * WordReduce继承自 Reducer<Text,IntWritable,Text,IntWritable>  
   * [不管几个map,都只有一个reduce,这是一个汇总] 
   * reduce[循环所有的map值,把word ==> one 的Key-Value对进行汇总] 
    * 这里的Key为Mapper设置的word[每一个Key-Value都会有一次reduce] 
   *  当循环结束后，最后的确context就是最后的结果.  
   * @author jayliu 
   */  
  public   class WordReduce  
       extends Reducer<Text,IntWritable,Text,IntWritable> {  
    private IntWritable result = new IntWritable();  
    public void reduce(Text key, Iterable<IntWritable> values,  
                       Context context  
                       ) throws IOException, InterruptedException {  
      int sum = 0;  
      for (IntWritable val : values) {  
        sum += val.get();  
      }  
      result.set(sum);  
      context.write(key, result);  
    }  
  }   
