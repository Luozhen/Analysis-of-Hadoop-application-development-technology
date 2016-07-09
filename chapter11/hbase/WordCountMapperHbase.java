package mapred.hbase;
public  class  WordCountMapperHbase  extends Mapper<Object, Text, Text, IntWritable>{   
    private final static IntWritable one = new IntWritable(1);  
    private Text word = new Text();     
    //map函数没有改变  
    public void map(Object key, Text value, Context context  
                    ) throws IOException, InterruptedException {  
      StringTokenizer itr = new StringTokenizer(value.toString());  
      while (itr.hasMoreTokens()) {  
        word.set(itr.nextToken());  
        context.write(word, one);  
      }  
    }  
