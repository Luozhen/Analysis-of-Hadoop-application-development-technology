package mapreduce;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class WordCountTest {
	private Mapper mapper;
	private Reducer reducer;
	private MapReduceDriver driver;
	@Before
	public void init() {
		mapper = new Wordcount.TokenizerMapper();
		reducer = new Wordcount.IntSumReducer();
		driver = new MapReduceDriver(mapper, reducer);
	}
	@Test
	public void test() throws RuntimeException, IOException {
		String line = "easyhadoop is a great open source of hadoop is it";
		driver.withInput("", new Text(line))
		        .withOutput(new Text("a"), new IntWritable(1))
				.withOutput(new Text("easyhadoop"), new IntWritable(1))
				.withOutput(new Text("great"), new IntWritable(1))
				.withOutput(new Text("hadoop"), new IntWritable(1))
				.withOutput(new Text("is"), new IntWritable(2))
				.withOutput(new Text("it"), new IntWritable(1))
				.withOutput(new Text("of"), new IntWritable(1))
			    .withOutput(new Text("open"), new IntWritable(1))
				.withOutput(new Text("source"), new IntWritable(1))
				.runTest();
	}
}
