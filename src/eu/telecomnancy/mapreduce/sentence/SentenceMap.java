package eu.telecomnancy.mapreduce.sentence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

import eu.telecomnancy.mapreduce.vendor.text.StanfordParser;

public class SentenceMap extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf		= context.getConfiguration();
		Gson gson				= new Gson();
		
		java.util.Map<?, ?> publication = gson.fromJson(value.toString(), HashMap.class);
		StanfordParser parser			= new StanfordParser();
		List<String> sentences			= new ArrayList<String>();
		
		sentences.add((String) publication.get("title"));
		sentences.addAll(parser.split((String) publication.get("abstrct")));
		
		Text text	= new Text();
		Text k		= new Text();
		for (int i=0; i<sentences.size(); i++) {
			String id = String.format("%s_%s", publication.get("_id"), i);
			java.util.Map<String, String> json = new HashMap<String, String>();
			json.put("_id", id);
			json.put("pid", conf.get("factapgx.pid"));
			json.put("year", conf.get("factapgx.year"));
			json.put("sentence", sentences.get(i));
			
			text.set(gson.toJson(json));
			
			k.set(id);
			context.write(k, text);
		}
	}
}