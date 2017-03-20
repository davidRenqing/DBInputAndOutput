package cn.itcast.hadoop.mr.wordcount;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;



//4个泛型中，前两个制定mapper输入数据的类型，KEYIN 是输入的key 的类型，VALUEIN 是输入的 value 的类型。
//KEYOUT VALUEOUT 是map函数的输出的类型
//map函数进来的形参 KEYIN VALUEIN，我们是不能控制的，那么我们根据框架一致就好了，框架传给我们什么我们就用什么泛型就好了。
//默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的起始偏移量。这一行的内容作为 value
//KEYIN : long ;VALUEIN : string
//输出：String ；输出的内容是：Long 类型的数据
//java中使用网络传输，要将数据进行序列化；hadoop不使用java的序列化的方法；hadoop有自己的序列化的接口，可以少很多附加的信息


public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	//Mapper 类要重新 map这个方法，每一行的数据调用一次 map这个方法
		//mapreduce 框架每读一行数据就调用一次该
	

		
	
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//具体业务逻辑就写在这个方法体中，而且我们业务要处理的数据已经被框架传递进来，在方法的参数中 key-value 
			// key 是一行数据的起始偏移量。value是这一行的文本内容
			
//			Text outcome=tranformText(value, "GB2314");
//			String line=outcome.toString();
//			String line=decode(value.toString());
		
			//1.首先是切分单词
//			String line = value.toString();             //将传进来的文件的一行，value转换成 string类型的数据
			
			System.out.println(value.toString());
			String[] words=StringUtils.split(value.toString(), " ");              
			
			
			//遍历这个单词数组，输出为 kv形式 k:单词 v:1
			for(String word:words)
			{
				
				// 使用这个输出的工具 context 将word 输出出去
				System.out.println(word);
				context.write(new Text(word), new LongWritable(1));         //将这一行输出过程 
			}
		} 
		
		
		@Test
		void test()
		{
			boolean empty = StringUtils.isEmpty("geng");
			System.out.println(empty);
		}
}
