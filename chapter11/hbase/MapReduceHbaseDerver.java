package mapred.hbase;
public  class  MapReduceHbaseDerver{
public static void main(String[] args) throws Exception {  
      String tablename  = "wordcount";  
      //ʵ����Configuration��ע�ⲻ���� new HBaseConfiguration()�ˡ�  
     Configuration conf = HBaseConfiguration.create();   
     HBaseAdmin admin = new HBaseAdmin(conf);  
     if(admin.tableExists(tablename)){  
         System.out.println("table exists! recreating ...");  
         admin.disableTable(tablename);  
         admin.deleteTable(tablename);  
     }  
     HTableDescriptor htd = new HTableDescriptor(tablename);  
     HColumnDescriptor hcd = new HColumnDescriptor("content");  
     htd.addFamily(hcd);        //��������  
     admin.createTable(htd);    //������  
       
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
    if (otherArgs.length != 1) {  
      System.err.println("Usage: wordcount <in> <out>"+otherArgs.length);  
      System.exit(2);  
    }  
    Job job = new Job(conf, "word count");  
    job.setJarByClass(MapReduceHbaseDerver.class);  
    job.setMapperClass(WordCountMapperHbase .class);  
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
    //�˴���TableMapReduceUtilע��Ҫ��hadoop.hbase.mapreduce���еģ�������hadoop.hbase.mapred���е�  
    TableMapReduceUtil.initTableReducerJob(tablename, WordCountReducerHbase.class, job);        
    //key��value�������趨��÷���initTableReducerJob�������棬����ᱨ��  
    job.setOutputKeyClass(Text.class);  
    job.setOutputValueClass(IntWritable.class);      
    System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }  
