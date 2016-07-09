package mapred.mysql;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DBAccessMapper extends MapReduceBase implements
            Mapper<LongWritable, TeacherRecord, LongWritable, Text> {
        @Override
        public void map(LongWritable key, TeacherRecord value,
                OutputCollector<LongWritable, Text> collector, Reporter reporter)
                throws IOException {
            // TODO Auto-generated method stub
            new collector.collect(new LongWritable(value.id), new Text(value
                    .toString()));
        }
    }
