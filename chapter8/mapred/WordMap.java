package mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/** 
   * WordMap������ Mapper<Object, Text, Text, IntWritable> 
   * [һ���ļ���һ��Map,�����ļ��ͻ�������Map] 
   * Map[������������ļ����� ��" \t\n\r\f" ���зָȻ������ word ==> one ��Key-Value��] 
   * @param Object  Input key Type: 
   * @param Text    Input value Type: 
   * @param Text    Output key Type: 
   * @param IntWritable Output value Type: 
   * 
   * Writable����Ҫ�ص�����ʹ��Hadoop���֪����һ��Writable���͵Ķ�����������Serialize�Լ�Deserialize. 
   * WritableComparable��Writable�Ļ�����������compareTo�ӿڣ�ʹ��Hadoop���֪��������WritableComparable���͵Ķ����������  
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
