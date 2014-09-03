package eu.telecomnancy.mapreduce.relation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelationReduce extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text v = new Text();
		String out = "";
		for (Text value : values) {
			out += value.toString() + ",";
		}
		
		v.set(out.substring(0, out.length()-1));
		context.write(key, v);
	}
}
