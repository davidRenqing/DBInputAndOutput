package cn.itcast.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

//这个类用来描述一个特定的作业
//比如，该作业使用哪个类作为逻辑处理中的map，哪个作为 reduce
//还可以制定该作业所处理的数据所在的路径 	
//还可以制定改作业输出的结果放到哪个路径

public class WCRunner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//1.创建job对象
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		//7.指定我这个job运行的所有的类所在的包在哪gi	
		//这个函数的作用是，job先找到 WCRunner 这个类在哪个包，然后在这个包中寻找 WCMapper 和 WCReducer 这写类 
		job.setJarByClass(WCRunner.class);
		
		//2.设置这个job 用哪个类作为 maper
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);                    //设定本job使用的reduce 过程使用的类
		
		//3.指定reduce的输出数据的<key,value> 类型
		job.setOutputKeyClass(Text.class);                      //设定reduce 的输出的 key2 的类型
		job.setOutputValueClass(LongWritable.class);            //设定 reduce 的输出的 value2 的类型
		
		
		//4.指定 map 的输出数据的 <key,value> 的类型
		job.setMapOutputKeyClass(Text.class);                  //设定map的输出的 key1 的类型
		job.setMapOutputValueClass(LongWritable.class);        //设定 reduce过程的输出的 value1 的类型
		
		
		//如果原始的输出路径存在就删除
		Path out=new Path("/output");
		out.getFileSystem(conf).delete(out,true); //确实删除了
		FileInputFormat.setInputPaths(job,new Path("/input"));       //注意这个 path对象，你只需要指定目录就可以了。它会把这个目录下的所有的文件都一次进行map计算的

		
		
		//有几个reduce 就会有几个输出文件
		FileOutputFormat.setOutputPath(job, out); 
		
		//8.提交这个jobcle	
		//如果job的形参是true，则将这个job执行的过程显示出来。如果这个参数是 false ，就不将这个job的执行过程展示出来
		job.waitForCompletion(true);
	}
}
