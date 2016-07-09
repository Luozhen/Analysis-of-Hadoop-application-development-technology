package io;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
public class SeqWrite {
    private static final String[] data = { "a,b,c,d,e,f,g", "h,i,j,k,l,m,n",
            "o,p,q,r,s,t", "u,v,w,x,y,z", "0,1,2,3,4", "5,6,7,8,9," };
    // Write an Sequrence File
    public static void main(String[] args) throws Exception {
        // 获取Configuration实例
        Configuration conf = new Configuration();
        // HDFS 文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        // 序列化文件路径
        Path path = new Path("test.seq");
        // 打开SequenceFile.Writer对象，写入键值
        SequenceFile.Writer writer = null;
        IntWritable key = new IntWritable();
       Text value = new Text();
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
                    value.getClass());
           for (int i = 0; i < 10000; i++) {
                key.set(i);
               value.set(SeqWrite.data[i % SeqWrite.data.length]);
                writer.append(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           IOUtils.closeStream(writer);
        }
    }
}
