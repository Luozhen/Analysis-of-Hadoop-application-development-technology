package mapred.hbase;
public  class WordCountMapperHbaseReader
       extends TableMapper<Text, Text>{     
    public void map(ImmutableBytesWritable row, Result values, Context context  
                    ) throws IOException, InterruptedException {  
        StringBuffer sb = new StringBuffer("");  
        for(java.util.Map.Entry<byte[],byte[]> value : values.getFamilyMap(  
                "content".getBytes()).entrySet()){  
            String str = new String(value.getValue());  //���ֽ�����ת����String���ͣ���Ҫnew String();  
            if(str != null){  
                sb.append(new String(value.getKey()));  
                sb.append(":");  
                sb.append(str);  
            }  
            context.write(new Text(row.get()), new Text(new String(sb)));       
        }  
     }  
  }    
