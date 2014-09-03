package eu.telecomnancy.mapreduce.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;

public class EntityReduce extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Gson gson									= new Gson();
		java.util.Map<String, Object> results = new HashMap<String, Object>();
		
		for (Text value : values) {
			java.util.Map<?, ?> json = gson.fromJson(value.toString(), HashMap.class);
			
			for (Entry<?, ?> e : json.entrySet()) {
				List<String> ids	= new ArrayList<String>();
				
				if (e.getValue() instanceof List) {
					List<?> list		= (List<?>) e.getValue();
					for (Object o : list) {
						ids.add((String) o);
					}
					results.put((String) e.getKey(), ids);
				} else {
					results.put((String) e.getKey(), e.getValue());
				}
			}
		}
		
		Text value = new Text();
		value.set(gson.toJson(results));
		context.write(key, value);
	}
}