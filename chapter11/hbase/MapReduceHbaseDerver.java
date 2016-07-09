package mapred.hbase;
public  class  MapReduceHbaseDerver{
public static void main(String[] args) throws Exception {  
      String tablename  = "wordcount";  
      //实例化Configuration，注意不能用 new HBaseConfiguration()了。  
     Configuration conf = HBaseConfiguration.create();   
     HBaseAdmin admin = new HBaseAdmin(conf);  
     if(admin.tableExists(tablename)){  
         System.out.println("table exists! recreating ...");  
         admin.disableTable(tablename);  
         admin.deleteTable(tablename);  
     }  
     HTableDescriptor htd = new HTableDescriptor(tablename);  
     HColumnDescriptor hcd = new HColumnDescriptor("content");  
     htd.addFamily(hcd);        //创建列族  
     admin.createTable(htd);    //创建表  
       
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
    if (otherArgs.length != 1) {  
      System.err.println("Usage: wordcount <in> <out>"+otherArgs.length);  
      System.exit(2);  
    }  
    Job job = new Job(conf, "word count");  
    job.setJarByClass(MapReduceHbaseDerver.class);  
    job.setMapperClass(WordCountMapperHbase .class);  
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
    //此处的TableMapReduceUtil注意要用hadoop.hbase.mapreduce包中的，而不是hadoop.hbase.mapred包中的  
    TableMapReduceUtil.initTableReducerJob(tablename, WordCountReducerHbase.class, job);        
    //key和value到类型设定最好放在initTableReducerJob函数后面，否则会报错  
    job.setOutputKeyClass(Text.class);  
    job.setOutputValueClass(IntWritable.class);      
    System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }  
