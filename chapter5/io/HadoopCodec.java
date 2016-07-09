package io; 
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
public class HadoopCodec { 
    public static void main(String[] args) throws Exception { 
        // TODO Auto-generated method stub 
        String inputFile = "bigfile.txt"; 
        String outputFolder = "hdfs://192.168.129.35:9000/user/Hadoop-user/codec/"; 
        // String outputFile="bigfile.gz"; 
        // ��ȡHadoop�ļ�ϵͳ������ 
        Configuration conf = new Configuration(); 
        conf.set("Hadoop.job.ugi", "hadoop-user,hadoop-user"); 
        //���Ը���ѹ����ʽ��Ч�� 
        //gzip 
        long gzipTime = copyAndZipFile(conf, inputFile, outputFolder, "org.apache.Hadoop.io.compress.GzipCodec", "gz"); 
        //bzip2 
        long bzip2Time = copyAndZipFile(conf, inputFile, outputFolder, "org.apache.Hadoop.io.compress.BZip2Codec", "bz2"); 
        //deflate 
        long deflateTime = copyAndZipFile(conf, inputFile, outputFolder, "org.apache.Hadoop.io.compress.DefaultCodec", "deflate");  
        System.out.println("��ѹ�����ļ���Ϊ�� "+inputFile); 
        System.out.println("ʹ��gzipѹ����ʱ��Ϊ�� "+gzipTime+"����!"); 
        System.out.println("ʹ��bzip2ѹ����ʱ��Ϊ�� "+bzip2Time+"����!"); 
        System.out.println("ʹ��deflateѹ����ʱ��Ϊ�� "+deflateTime+"����!"); 
    } 
    public static long copyAndZipFile(Configuration conf, String inputFile, String outputFolder, String codecClassName, 
            String suffixName) throws Exception { 
        long startTime = System.currentTimeMillis(); 
        // ��Ϊ�����ļ�ϵͳ�ǻ���java.io���ģ����Դ���һ�������ļ������� 
        InputStream in = new BufferedInputStream(new FileInputStream(inputFile)); 
        //ȥ����չ����ȡbasename 
        String baseName = inputFile.substring(0, inputFile.indexOf(".")); 
        //��������ļ���������·����+������+��չ�� 
        String outputFile = outputFolder + baseName + "."+suffixName; 
         
        FileSystem fs = FileSystem.get(URI.create(outputFile), conf); 
        // ����һ�������������ͨ��������Ƹ��ݴ������������̬������ʵ�� 
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(codecClassName), conf); 
        // ����һ��ָ��HDFSĿ���ļ���ѹ���ļ������ 
        OutputStream out = codec.createOutputStream(fs.create(new Path(outputFile))); 
        // ��IOUtils���߽��ļ��ӱ����ļ�ϵͳ���Ƶ�HDFSĿ���ļ��� 
        try { 
            IOUtils.copyBytes(in, out, conf); 
        } finally { 
            IOUtils.closeStream(in); 
            IOUtils.closeStream(out); 
        }  
        long endTime = System.currentTimeMillis();  
        return endTime - startTime; 
    } 
}
