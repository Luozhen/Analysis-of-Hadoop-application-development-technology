package mapred.hbase;
public  class WordCountReducerHbase extends TableReducer <Text,IntWritable,ImmutableBytesWritable> {  
  private IntWritable result = new IntWritable();    
  public void reduce(Text key, Iterable<IntWritable> values,   
                     Context context  
                     ) throws IOException, InterruptedException {  
    int sum = 0;  
    for (IntWritable val : values) {  
      sum += val.get();  
    }  
    result.set(sum);  
    Put put = new Put(key.getBytes());  //putʵ������ÿһ���ʴ�һ��  
    //����Ϊcontent�������η�Ϊcount����ֵΪ��Ŀ  
    put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));  
    context.write(new ImmutableBytesWritable(key.getBytes()), put);  
  }  
}   
