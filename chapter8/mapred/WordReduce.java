package mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
	/** 
   * WordReduce�̳��� Reducer<Text,IntWritable,Text,IntWritable>  
   * [���ܼ���map,��ֻ��һ��reduce,����һ������] 
   * reduce[ѭ�����е�mapֵ,��word ==> one ��Key-Value�Խ��л���] 
    * �����KeyΪMapper���õ�word[ÿһ��Key-Value������һ��reduce] 
   *  ��ѭ������������ȷcontext�������Ľ��.  
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
