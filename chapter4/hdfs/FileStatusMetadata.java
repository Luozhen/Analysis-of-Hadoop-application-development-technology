package hdfs;
import java.net.URI; 
import java.sql.Timestamp; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileStatus; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 

public class FileStatusMetadata { 
    public static void main(String[] args) throws Exception { 
        //��ȡhadoop�ļ�ϵͳ������ 
        Configuration conf = new Configuration(); 
        conf.set("hadoop.job.ugi", "jayliu,jayliu"); 
        //ʵ��1:�鿴HDFS��ĳ�ļ���Ԫ��Ϣ 
        System.out.println("ʵ��1:�鿴HDFS��ĳ�ļ���Ԫ��Ϣ"); 
        String fileUri = args[0]; 
        FileSystem fileFS = FileSystem.get(URI.create(fileUri) ,conf); 
        FileStatus fileStatus = fileFS.getFileStatus(new Path(fileUri)); 
        //��ȡ����ļ��Ļ�����Ϣ       
        if(fileStatus.isDir()==false){ 
            System.out.println("���Ǹ��ļ�"); 
        } 
        System.out.println("�ļ�·��: "+fileStatus.getPath()); 
        System.out.println("�ļ�����: "+fileStatus.getLen()); 
        System.out.println("�ļ��޸����ڣ� "+new Timestamp (fileStatus.getModificationTime()).toString()); 
        System.out.println("�ļ��ϴη������ڣ� "+new Timestamp(fileStatus.getAccessTime()).toString()); 
        System.out.println("�ļ��������� "+fileStatus.getReplication()); 
        System.out.println("�ļ��Ŀ��С�� "+fileStatus.getBlockSize()); 
        System.out.println("�ļ������ߣ�  "+fileStatus.getOwner()); 
        System.out.println("�ļ����ڵķ��飺 "+fileStatus.getGroup()); 
        System.out.println("�ļ��� Ȩ�ޣ� "+fileStatus.getPermission().toString()); 
        System.out.println(); 
         
        //ʵ��2:�鿴HDFS��ĳ�ļ���Ԫ��Ϣ 
        System.out.println("ʵ��2:�鿴HDFS��ĳĿ¼��Ԫ��Ϣ"); 
        String dirUri = args[1]; 
        FileSystem dirFS = FileSystem.get(URI.create(dirUri) ,conf); 
        FileStatus dirStatus = dirFS.getFileStatus(new Path(dirUri)); 
        //��ȡ���Ŀ¼�Ļ�����Ϣ       
        if(dirStatus.isDir()==true){ 
            System.out.println("���Ǹ�Ŀ¼"); 
        } 
        System.out.println("Ŀ¼·��: "+dirStatus.getPath()); 
        System.out.println("Ŀ¼����: "+dirStatus.getLen()); 
        System.out.println("Ŀ¼�޸����ڣ� "+new Timestamp (dirStatus.getModificationTime()).toString()); 
        System.out.println("Ŀ¼�ϴη������ڣ� "+new Timestamp(dirStatus.getAccessTime()).toString()); 
        System.out.println("Ŀ¼�������� "+dirStatus.getReplication()); 
        System.out.println("Ŀ¼�Ŀ��С�� "+dirStatus.getBlockSize()); 
        System.out.println("Ŀ¼�����ߣ�  "+dirStatus.getOwner()); 
        System.out.println("Ŀ¼���ڵķ��飺 "+dirStatus.getGroup()); 
        System.out.println("Ŀ¼�� Ȩ�ޣ� "+dirStatus.getPermission().toString()); 
        System.out.println("���Ŀ¼�°��������ļ���Ŀ¼��"); 
        for(FileStatus fs : dirFS.listStatus(new Path(dirUri))){ 
            System.out.println(fs.getPath()); 
        } 
    } 
}
