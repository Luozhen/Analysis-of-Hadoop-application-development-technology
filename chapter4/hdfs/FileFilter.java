package hdfs;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
/**
 * ����������ļ�������ʵ��
 * @author muyannian
 */
public  class FileFilter implements PathFilter {
	private String matcher = "*";
	private FileSystem fs = null;
	public void setFileSystem(FileSystem _fs) {
		fs = _fs;
	}
	public void setMatcher(String sMatcher) {
		matcher = sMatcher;
	}
	public boolean accept(String filePath)
	{
		// �ļ�·������
		boolean isMatcherAll = matcher.equals("*");
		boolean isStrIndexOf = (filePath.indexOf(matcher) >= 0);
		if (isMatcherAll || isStrIndexOf) {
			return true;
		}
		try {
			if (filePath.matches(matcher)) {
				return true;
			}
		} catch (Exception e) {
			System.err.println(FileFilter.class.getName() + "err for"
					+ matcher);
			return true;
		}
		return false;
	}
	public boolean accept(Path path) {
		// ���������ļ��������ļ���
		String fileName = path.getName();
		if (fileName.endsWith(".temp")
				|| fileName.endsWith(".crc")) {
			return false;
		}
		// �����Ŀ¼����������������
		try {
			if(HadoopUtil.exists(path.toString(), fs))
			{
				if (fs.getFileStatus(path).isDir()) {
					return true;
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return true;
		}
		return accept( path.toString());
	}
}
