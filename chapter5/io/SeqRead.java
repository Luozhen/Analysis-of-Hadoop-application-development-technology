package io;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
public class SeqRead {
    public static void main(String[] args) throws Exception {
        // Configuration
        Configuration conf = new Configuration();
        // HDFS File Sytem
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),
                conf);
        // Seq File Path
        Path path = new Path("test.seq");
        // Read SequenceFile
        SequenceFile.Reader reader = null;
        try {
            // Get Reader
            reader = new SequenceFile.Reader(fs, path, conf);
            // Get Key/Value Class
            Writable key = (Writable) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            // Read each key/value
            while (reader.next(key, value)) {
                System.out.println(key + "\t" + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
