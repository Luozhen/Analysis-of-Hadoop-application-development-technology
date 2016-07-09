package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
  public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  public class Context  extends MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    public Context(Configuration conf, TaskAttemptID taskid,
                   RecordReader<KEYIN,VALUEIN> reader,
                   RecordWriter<KEYOUT,VALUEOUT> writer,
                   OutputCommitter committer,
                   StatusReporter reporter,
                   InputSplit split) throws IOException, InterruptedException {
      super(conf, taskid, reader, writer, committer, reporter, split);
    }
  } 
  /**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }
  /**
   * Called once for each key/value pair in the input split. Most applications
   * should override this, but the default is the identity function.
   */
  @SuppressWarnings("unchecked")
  protected void map(KEYIN key, VALUEIN value, 
                     Context context) throws IOException, InterruptedException {
    context.write((KEYOUT) key, (VALUEOUT) value);
  }
  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }
  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * @param context
   * @throws IOException
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
      map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
  }
}
