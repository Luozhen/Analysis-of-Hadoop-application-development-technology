package hdfs;
import java.io.BufferedInputStream;  
import java.io.BufferedOutputStream;  
import java.io.File;  
import java.io.FileInputStream;  
import java.io.FileOutputStream;  
import java.io.IOException;  
import java.io.InputStream;  
import java.io.OutputStream;  
import java.net.URI;  
import java.net.URL;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import org.apache.log4j.Logger;   
@SuppressWarnings("deprecation")  
public class HDFSDataInputExample {   
    static final Logger logger = Logger.getLogger(HDFSSample.class);  
    static {  
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());  
    }   
    public static void main(String[] args) throws Exception {  
        HDFSSample sample = new HDFSSample();  
        String cmd = args[0];  
        String localPath = args[1];  
        String hdfsPath = args[2];  
        if (cmd.equals("create")) {  
            sample.createFile(localPath, hdfsPath);  
        } else if (cmd.equals("get")) {  
            boolean print = Boolean.parseBoolean(args[3]);  
            sample.getFile(localPath, hdfsPath, print);  
        }  
    }    
    /** 
     * 创建文件 
     * @param localPath 
     * @param hdfsPath 
     * @throws IOException 
     */  
    @SuppressWarnings("deprecation")  
    public void createFile(String localPath, String hdfsPath) throws IOException {  
        InputStream in = null;  
        try {  
            Configuration conf = new Configuration();  
            FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);  
            FSDataOutputStream out = fileSystem.create(new Path(hdfsPath));  
            in = new BufferedInputStream(new FileInputStream(new File(localPath)));  
            IOUtils.copyBytes(in, out, 4096, false);  
            out.hsync();  
            out.close();  
            logger.info("create file in hdfs:" + hdfsPath);  
        } finally {  
            IOUtils.closeStream(in);  
        }  
    }  
    /** 
     * 从HDFS获取文件   
     * @param localPath 
     * @param hdfsPath 
     * @throws IOException 
     */  
    public void getFile(String localPath, String hdfsPath, boolean print) throws IOException {  
        Configuration conf = new Configuration();  
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);  
        FSDataInputStream in = null;  
        OutputStream out = null;  
        try {  
            in = fileSystem.open(new Path(hdfsPath));  
            out = new BufferedOutputStream(new FileOutputStream(new File(localPath)));  
            IOUtils.copyBytes(in, out, 4096, !print);  
            logger.info("get file form hdfs to local: " + hdfsPath + ", " + localPath);  
            if (print) {  
                in.seek(0);  
                IOUtils.copyBytes(in, System.out, 4096, true);  
            }  
        } finally {  
            IOUtils.closeStream(out);  
        }  
    }  
}  
