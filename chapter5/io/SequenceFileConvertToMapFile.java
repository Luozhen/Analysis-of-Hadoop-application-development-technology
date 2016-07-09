package io;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
public class SequenceFileConvertToMapFile {
 private static final String data[]={
  "One, two, buckle my shoe",
  "Three, four, shut the door",
  "Five, six, pick up sticks",
  "Seven, eight, lay them straight",
  "Nine, ten, a big fat hen"
 };
  public static class MapFileFixerTest{
  public void testMapFix(String sqFile) throws Exception{
   String uri=sqFile;
   Configuration conf=new Configuration();
   FileSystem fs=FileSystem.get(URI.create(uri),conf);
   Path path=new Path(uri);
   Path mapData=new Path(path,MapFile.DATA_FILE_NAME);
   SequenceFile.Reader reader=new SequenceFile.Reader(fs, mapData, conf);
   Class keyClass=reader.getKeyClass();
   Class valueClass=reader.getValueClass();
   reader.close();
   long entries=MapFile.fix(fs, path, keyClass, valueClass, false, conf);
   System.out.printf("create MapFile from sequenceFile about %d entries!",entries);
  }
 }
 public static void main(String[] args) throws Exception{
    SequenceFileConvertToMapFile fixer=new SequenceFileConvertToMapFile ();
    fixer.testMapFix(args[0]);
  }
} 
