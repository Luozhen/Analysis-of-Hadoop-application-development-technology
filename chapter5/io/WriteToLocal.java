package io;
import java.io.IOException;  
import java.io.OutputStream;  
import java.net.URI;  
import java.net.URISyntaxException;   
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.LocalFileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.fs.RawLocalFileSystem;    
public class WriteToLocal {  
    public static void main(String[] args) throws IOException, URISyntaxException {  
        Configuration conf = new Configuration();  
        LocalFileSystem fs =  new LocalFileSystem(new RawLocalFileSystem());  
        fs.initialize(new URI("file:///home/jayliu/file1.txt"), conf); 
        OutputStream out = fs.create(new Path("file:/// home/jayliu/file1.txt "));  
        for(int i = 0; i < 512*10;i++){  
            out.write(97);  
        }  
        out.close();  
        Path file = fs.getChecksumFile(new Path("file:/// home/jayliu/file1.txt "));  
        System.out.println(file.getName());  
        fs.close();  
    }  
}  
