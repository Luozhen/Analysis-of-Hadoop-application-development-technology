package mapred.mysql;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DBAccessReader {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(DBAccessReader.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(DBInputFormat.class);
        FileOutputFormat.setOutputPath(conf, new Path("/user/hadoop/mysql/output/"));
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost/school","root","root");
        String [] fields = {"id", "name", "gender", "number"};
        DBInputFormat.setInput(conf, StudentRecord.class,"Student","id", fields)
        conf.setMapperClass(DBAccessMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        JobClient.runJob(conf);
        }
}
