package cn.itcast.hadoop.hdfs;

//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.net.URI;
//
//import org.apache.commons.io.IOUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.RemoteIterator;
//import org.junit.Before;
//import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection;
import org.apache.hadoop.util.Options.FSDataInputStreamOption;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {
	
	FileSystem fs = null;


	@Test
	public void upload() throws Exception {

		Configuration conf = new Configuration();

		
		FileSystem fs = FileSystem.get(conf);
		System.out.println(conf);
		Path dst = new Path("hdfs://hadoop01:9000/c.gz");
		
		FSDataOutputStream os = fs.create(dst);
		
		FileInputStream is = new FileInputStream("/home/hadoop01/app/jdk-8u111-linux-x64.tar.gz");
		
		IOUtils.copy(is, os);
		

	}

	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		
		
		FileSystem fs = FileSystem.get(conf);
		System.out.println(conf);
		FSDataInputStream is=fs.open(new Path("/z.gz"));
		
		FileOutputStream os = new FileOutputStream("/home/hadoop01/renqing/a.zip");
		
//		FileInputStream is = new FileInputStream("/home/hadoop01/app/jdk-8u111-linux-x64.tar.gz");
		
		IOUtils.copy(is, os);
		
	}
	

	
	
	
	
	
}
