package cn.itcast.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//定义4个泛型。KEYIN，VALUEIN 是map输出的 <key,value> 类型。
//输出的时候是 单词的名字和总次数

public class WCReducer extends Reducer<Text,LongWritable, Text, LongWritable> {
	//在这个类当中，要复写一个hadoop当中的 Reducer类当中的 reducer 方法
	
	
	//这个 reduce函数传进的参数Text是每个单词的名字。
	//Iterable<LongWritable> arg1 是每一个单词的数组。是一个迭代器。<hello,{1，1，1，1，1，1，1，1} > 这样的形式
	//框架在map处理完成之后，将所有的kv缓存起来，进行分组，然后传递一个组<key,value{}>,调用一次 reduce方法 
	@Override
	protected void reduce(Text key1, Iterable<LongWritable> value1s,Context context) throws IOException, InterruptedException {
	   
		long count=0;
		
		//便利values的list，进行累加求和
		for(LongWritable value:value1s)
		{
			count+=value.get();    //value 的类型是long类型的，调用这个get 方法，将单词进行累加
		}
		
		//输出一个单词的结果输出，使用 context 这个对象将所有的结果进行输出
		context.write(key1, new LongWritable(count));
	}
}
