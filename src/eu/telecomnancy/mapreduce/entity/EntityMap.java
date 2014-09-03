package eu.telecomnancy.mapreduce.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import eu.telecomnancy.mapreduce.SentenceParser;

public class EntityMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf		= context.getConfiguration();
		
		Gson gson				= new Gson();
		String[] parts			= value.toString().split("\t");
		String json				= parts[1];
		
		java.util.Map<?, ?> s	= gson.fromJson(json, HashMap.class);
		
		String sentence			= (String) s.get("sentence");
		sentence				= sentence.toLowerCase().replaceAll("\\.+$", "").replaceAll("[^a-z0-9. -]", "");
		
		SentenceParser parser	= new SentenceParser(sentence);
		java.util.Map<Character, List<String>> words = parser.getWords();
		
		DBCursor cursor;
		
		// Parse diseases
		List<String> diseasesIds	= new ArrayList<String>();
		DB db						= (new Mongo(conf.get("mongo.server"), Integer.parseInt(conf.get("mongo.port")))).getDB(conf.get("mongo.db"));
		try {
			cursor						= db.getCollection("diseases").find();
			
			for (DBObject o : cursor) {
				String id			= (String) o.get("_id");
				BasicDBList enames	= (BasicDBList) o.get("disease");
				
				for (Object name : enames) {
					String n = name.toString().trim();
					if (n.isEmpty()) { continue; }
					
					char c = n.charAt(0);
					if (words.containsKey(c) && words.get(c).contains(n)) {
						if (!diseasesIds.contains(id)) {
							diseasesIds.add(id);
						}
						break;
					}
				}
			}
			
			// Parse drugs
			List<String> drugsIds	= new ArrayList<String>();
			cursor					= db.getCollection("drugs").find();
			for (DBObject o : cursor) {
				String id			= (String) o.get("_id");
				BasicDBList enames	= (BasicDBList) o.get("drug");
				
				for (Object name : enames) {
					String n = name.toString().trim();
					if (n.isEmpty()) { continue; }
					
					char c = n.charAt(0);
					if (words.containsKey(c) && words.get(c).contains(n)) {
						if (!drugsIds.contains(id)) {
							drugsIds.add(id);
						}
						break;
					}
				}
			}
			
			// Parse genes
			List<String> genesIds	= new ArrayList<String>();
			cursor					= db.getCollection("genes").find();
			for (DBObject o : cursor) {
				String id			= (String) o.get("_id");
				BasicDBList enames	= (BasicDBList) o.get("gene");
				
				for (Object name : enames) {
					String n = name.toString().trim();
					if (n.isEmpty()) { continue; }
					
					char c = n.charAt(0);
					if (words.containsKey(c) && words.get(c).contains(n)) {
						if (!genesIds.contains(id)) {
							genesIds.add(id);
						}
						break;
					}
				}
			}
			
			
			// Reduce data
			Text k					= new Text();
			Text v					= new Text();
			
			k.set((String) s.get("_id"));
			// Sentence informations
			v.set(json);
			context.write(k, v);
			
			// Diseases
			v.set(String.format("{\"disease\":%s}", gson.toJson(diseasesIds)));
			context.write(k, v);
			
			// Drugs
			v.set(String.format("{\"drug\":%s}", gson.toJson(drugsIds)));
			context.write(k, v);
			
			// Genes
			v.set(String.format("{\"gene\":%s}", gson.toJson(genesIds)));
			context.write(k, v);
		} catch (Exception e) {
			throw e;
		} finally {
			if (db != null) {
				db.getMongo().close();
			}
		}
	}
}