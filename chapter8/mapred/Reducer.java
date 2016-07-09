package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  public class Context extends ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    public Context(Configuration conf, TaskAttemptID taskid,
                   RawKeyValueIterator input, 
                   Counter inputKeyCounter,
                   Counter inputValueCounter,
                   RecordWriter<KEYOUT,VALUEOUT> output,
                   OutputCommitter committer,
                   StatusReporter reporter,
                   RawComparator<KEYIN> comparator,
                   Class<KEYIN> keyClass,
                   Class<VALUEIN> valueClass
                   ) throws IOException, InterruptedException {
      super(conf, taskid, input, inputKeyCounter, inputValueCounter,
            output, committer, reporter, 
            comparator, keyClass, valueClass);
    }
  }
  /**
   * Called once at the start of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }
  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method. The default implementation
   * is an identity function.
   */
  @SuppressWarnings("unchecked")
  protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context
                        ) throws IOException, InterruptedException {
    for(VALUEIN value: values) {
      context.write((KEYOUT) key, (VALUEOUT) value);
    }
  }
  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }
  /**
   * Advanced application writers can use the 
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
   * control how the reduce task works.
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
    }
    cleanup(context);
  }
}
