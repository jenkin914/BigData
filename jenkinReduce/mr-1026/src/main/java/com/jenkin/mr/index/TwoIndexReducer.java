package com.jenkin.mr.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{
	
	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
//		jenkin --a.txt	3
//				--b.txt	2
//				--c.txt	2
//		jenkin	c.txt-->2	b.txt-->2	a.txt-->3
		
		// 1 拼接字符串
		StringBuffer sb = new StringBuffer();
		
		for (Text value : values) {
			sb.append(value.toString().replace("\t", "-->") +"\t");
		}
		
		v.set(sb.toString());

		// 2 写出
		context.write(key, v);
	}
}
