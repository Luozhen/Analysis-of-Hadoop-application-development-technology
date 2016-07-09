package mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/** 
   * WordMap继续自 Mapper<Object, Text, Text, IntWritable> 
   * [一个文件就一个Map,两个文件就会有两个Map] 
   * Map[这里读入输入文件内容 以" \t\n\r\f" 进行分割，然后设置 word ==> one 的Key-Value对] 
   * @param Object  Input key Type: 
   * @param Text    Input value Type: 
   * @param Text    Output key Type: 
   * @param IntWritable Output value Type: 
   * 
   * Writable的主要特点是它使得Hadoop框架知道对一个Writable类型的对象怎样进行Serialize以及Deserialize. 
   * WritableComparable在Writable的基础上增加了compareTo接口，使得Hadoop框架知道怎样对WritableComparable类型的对象进行排序。  
   * @author jayliu  
   */  
  public  class WordMap
       extends Mapper<Object, Text, Text, IntWritable>{  
    private final static IntWritable one = new IntWritable(1);  
    private Text word = new Text();  
    public void map(Object key, Text value, Context context  
                    ) throws IOException, InterruptedException {  
      StringTokenizer itr = new StringTokenizer(value.toString());  
      while (itr.hasMoreTokens()) {  
        word.set(itr.nextToken());  
        context.write(word, one);  
      }  
    }
  }
