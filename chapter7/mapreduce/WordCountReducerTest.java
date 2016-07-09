package mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class WordCountReducerTest {
	private Reducer reducer;
	private ReduceDriver driver;
	@Before
	public void init() {
		reducer = new Wordcount.IntSumReducer();
		driver = new ReduceDriver(reducer);
	}
	@Test
	public void test() throws IOException {
		String key = "easyHadoop";
		List values = new ArrayList();
		values.add(new IntWritable(2));
		values.add(new IntWritable(5));
		driver.withInput(new Text("easyHadoop"), values)
				.withOutput(new Text("easyHadoop"), new IntWritable(7)).runTest();
	}
}
