package mapred.hbase;
public  class WordCountReduceHbaseReader
       extends Reducer <Text,Text,Text,Text> {  
    private Text result = new Text();  
  
    public void reduce(Text key, Iterable<Text> values,   
                       Context context  
                       ) throws IOException, InterruptedException {  
      for (Text val : values) {  
          result.set(val);  
          context.write(key,result);  
      }    
    }  
  }    
