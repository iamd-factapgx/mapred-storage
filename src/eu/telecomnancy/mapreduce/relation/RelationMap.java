package eu.telecomnancy.mapreduce.relation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class RelationMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Gson gson				= new Gson();
		String[] parts			= value.toString().split("\t");
		
		java.util.Map<?, ?> sentence	= gson.fromJson(parts[1], HashMap.class);
		
		List<?> diseases	= (List<?>) sentence.get("disease");
		List<?> drugs		= (List<?>) sentence.get("drug");
		List<?> genes		= (List<?>) sentence.get("gene");
		
		List<String> entities	= new ArrayList<String>();
		List<String> relations	= new ArrayList<String>();
		
		String e, r, i;
		for (Object o : diseases) {
			e = (String) o;
			if (!entities.contains(e)) {
				entities.add(e);
			}
			
			for (Object o2 : drugs) {
				r = (String) o2;
				
				if (!entities.contains(r)) {
					entities.add(r);
				}
				
				i = String.format("%s-%s-1", e, r);
				if (!relations.contains(i)) {
					relations.add(i);
				}
			}
			
			for (Object o2 : genes) {
				r = (String) o2;
				
				if (!entities.contains(r)) {
					entities.add(r);
				}
				
				i = String.format("%s-%s-2", e, r);
				if (!relations.contains(i)) {
					relations.add(i);
				}
			}
		}
		
		for (Object o : drugs) {
			e = (String) o;
			for (Object o2 : genes) {
				r = (String) o2;
				i = String.format("%s-%s-3", e, r);
				if (!relations.contains(i)) {
					relations.add(i);
				}
			}
		}
		
		Text k = new Text(), 
			 v = new Text();
		
		for (String entity : entities) {
			k.set(entity + "-0");
			v.set((String) sentence.get("_id"));
			context.write(k, v);
		}
		
		for (String relation : relations) {
			k.set(relation);
			v.set((String) sentence.get("_id"));
			context.write(k, v);
		}
	}
}