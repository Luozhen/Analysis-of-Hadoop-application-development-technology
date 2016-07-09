package hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class FileSystemReader {
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String uri = args[0];
		FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
		InputStream inputStream = null;
		try {
			inputStream = fileSystem.open(new Path(uri));
			IOUtils.copyBytes(inputStream, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(inputStream);
		}
	}
}
