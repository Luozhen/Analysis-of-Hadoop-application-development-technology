package mapred.hbase;
public  class  MapReduceReaderHbaseDerver{
public static void main(String[] args) throws Exception {  
 String tablename  = "wordcount";    
 //ʵ����Configuration��ע�ⲻ���� new HBaseConfiguration()�ˡ�  
Configuration conf = HBaseConfiguration.create();  
  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
  if (otherArgs.length != 2) {  
    System.err.println("Usage: wordcount <in> <out>"+otherArgs.length);  
    System.exit(2);  
  }  
  Job job = new Job(conf, "word count");  
  job.setJarByClass(MapReduceReaderHbaseDerver.class);  
  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
  job.setReducerClass(WordCountReduceHbaseReader.class);  
    
  //�˴���TableMapReduceUtilע��Ҫ��hadoop.hbase.mapreduce���еģ�������hadoop.hbase.mapred���е�  
  Scan scan = new Scan(args[0].getBytes());  
  TableMapReduceUtil.initTableMapperJob(tablename, scan, WordCountMapperHbaseReader.class, Text.class, Text.class, job);  
  System.exit(job.waitForCompletion(true) ? 0 : 1);  
} 
} 
