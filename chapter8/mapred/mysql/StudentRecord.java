package mapred.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class StudentRecord implements Writable, DBWritable{
            int id;
            String name;
            String gender;
            String number;
            @Override
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            this.id = in.readInt();
            this.gender = Text.readString(in);
            this.name = in.readString();
            this.number = in.readString();
        }
            @Override
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeInt(this.id);
            Text.writeString(out,this.name);
            out.writeInt(this.gender);
            out.writeInt(this.number);
        }
            
            @Override
        public void readFields(ResultSet result) throws SQLException {
            // TODO Auto-generated method stub
            this.id = result.getInt(1);
            this.name = result.getString(2);
            this.gender = result.getString(3);
            this.number = result.getString(4);
        }
            
            @Override
        public void write(PreparedStatement stmt) throws SQLException{
            // TODO Auto-generated method stub
            stmt.setInt(1, this.id);
            stmt.setString(2, this.name);
            stmt.setString(3, this.gender);
            stmt.setString(4, this.number);
        }
            @Override
        public String toString() {
            // TODO Auto-generated method stub
            return new String(this.name + " " + this.gender + " " +this.number);
        }
