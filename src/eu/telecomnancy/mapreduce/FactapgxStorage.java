package eu.telecomnancy.mapreduce;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import eu.telecomnancy.mapreduce.sentence.*;
import eu.telecomnancy.mapreduce.relation.*;
import eu.telecomnancy.mapreduce.entity.*;

public class FactapgxStorage {
	
	public static String year;
	public static String pid;
	private static Configuration conf;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.exit(1);
		}
		year 		= args[0];
		pid			= args[1];
		
		conf	= new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hduser");
		conf.set("hadoop.job.ugi", "hduser");
		conf.set("mongo.server", args[2]);
		conf.set("mongo.port", args[3]);
		conf.set("mongo.db", args[4]);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.defaultFS", "hdfs://namenode.hadoop.telecomnancy.univ-lorraine.fr:9000");
		conf.set("factapgx.pid", pid);
		conf.set("factapgx.year", year);
		
		if (sentenceJob()) {
			if (entityJob()) {
				if (relationsJob()) {
					Gson gson			= new Gson();
					FileSystem fs		= FileSystem.newInstance(new URI(conf.get("fs.defaultFS")), conf);
					DB db				= (new Mongo(conf.get("mongo.server"), Integer.parseInt(conf.get("mongo.port")))).getDB(conf.get("mongo.db"));
					
					/**
					 * Store sentences with entities
					 */
					Path path;
					String directory	= conf.get("fs.defaultFS") + String.format("/facta-pgx/mapreduce/entities/%s/%s", year, pid);
					String outputdir	= conf.get("fs.defaultFS") + String.format("/facta-pgx/storage/sentences/%s/%s", year, pid);
					String filename		= directory + "/part-r-00000";
					
					String line, json;
					
					// Recreate output dir
					path = new Path(outputdir);
					if (fs.exists(path)) {
						fs.delete(path, true);
					}
					fs.mkdirs(path);
					
					// Parse output
					path					= new Path(filename);
					BufferedReader reader	= new BufferedReader(new InputStreamReader(fs.open(path)));
					int i = 0;
					
					List<String> publicationDiseases	= new ArrayList<String>();
					List<String> publicationDrugs		= new ArrayList<String>();
					List<String> publicationGenes		= new ArrayList<String>();
					
					while (null != (line = reader.readLine())) {
						line = line.trim();
						if (!line.isEmpty()) {
							String[] parts			= line.split("\t");
							json					= parts[1].trim();
							
							Map<?, ?> sentence	= gson.fromJson(json, HashMap.class);
							
							List<?> diseases	= (List<?>) sentence.get("disease");
							List<?> drugs		= (List<?>) sentence.get("drug");
							List<?> genes		= (List<?>) sentence.get("gene");
							
							String e;
							for (Object o : diseases) {
								e = (String) o;
								if (!publicationDiseases.contains(e)) {
									publicationDiseases.add(e);
								}
							}
							
							for (Object o : drugs) {
								e = (String) o;
								if (!publicationDrugs.contains(e)) {
									publicationDrugs.add(e);
								}
							}
							
							for (Object o : genes) {
								e = (String) o;
								if (!publicationGenes.contains(e)) {
									publicationGenes.add(e);
								}
							}
							
							String outputfile		= outputdir + "/" + parts[0].trim();
							PrintWriter writer		= new PrintWriter(fs.create(new Path(outputfile)));
							writer.println(json);
							writer.flush();
							writer.close();
							i++;
						}
					}
					reader.close();
					
					json	= String.format("{\"_id\": \"%s\", \"sentences\": %s}", pid, i);
					
					db.eval(String.format("db.publications.update({\"_id\": \"%s\"}, %s, {upsert: true});", pid, json));
					
					
					// Elasticsearch
					
					List<String> diseasesNames	= new ArrayList<String>();
					List<String> drugsNames		= new ArrayList<String>();
					List<String> genesNames		= new ArrayList<String>();
					
					DBCursor cursor = db.getCollection("diseases").find(new BasicDBObject("_id", new BasicDBObject("$in", publicationDiseases)));
					while (cursor.hasNext()) {
						DBObject o			= cursor.next();
						BasicDBList	names	= (BasicDBList) o.get("disease");
						for (Object n : names) {
							String name= (String) n;
							if (!diseasesNames.contains(name)) {
								diseasesNames.add(name);
							}
						}
					}
					
					Map<String, Object> esMap = new HashMap<String, Object>();
					
					esMap.put("id", pid);
					esMap.put("year", Integer.parseInt(year));
					esMap.put("disease", diseasesNames);
					esMap.put("drug", drugsNames);
					esMap.put("gene", genesNames);
					
					String esJson = gson.toJson(esMap);
					
					StringEntity entity = new StringEntity(esJson, ContentType.create("application/json", Consts.UTF_8));
					entity.setChunked(true);
					
					DefaultHttpClient httpclient = new DefaultHttpClient();
					HttpPut put = new HttpPut("http://elasticsearch.telecomnancy.univ-lorraine.fr/factapgx/publications/");
					put.setEntity(entity);
					HttpResponse response1 = httpclient.execute(put);
					try {
					    HttpEntity entity1 = response1.getEntity();
					    EntityUtils.consume(entity1);
					} finally {
						put.releaseConnection();
					}
					
					// Delete mapreduce job output
					path				= new Path(directory);
					fs.delete(path, true);
					
					
					/**
					 * Store relations
					 */
					directory		= conf.get("fs.defaultFS") + String.format("/facta-pgx/mapreduce/relations/%s/%s", year, pid);
					filename		= directory + "/part-r-00000";
					
					// Parse output
					path	= new Path(filename);
					reader	= new BufferedReader(new InputStreamReader(fs.open(path)));
					
					while (null != (line = reader.readLine())) {
						line = line.trim();
						if (!line.isEmpty()) {
							String[] parts			= line.split("\t");
							DBCollection relations	= db.getCollection("relations");
							long count = relations.count(new BasicDBObject("_id", parts[0]));
							if (count == 0) {
								String[] id = parts[0].split("-"), entities;
								String type;
								if (id.length == 3) {
									entities = new String[]{id[0], id[1]};
									type = id[2];
								} else if (id.length == 2) {
									entities = new String[]{id[0]};
									type = id[1];
								} else {
									entities = new String[]{parts[0]};
									type = "0";
								}
								
								db.eval(String.format("db.relations.insert({\"_id\": \"%s\", \"relations\": %s, \"type\": %s, \"sentences\": []})", parts[0], gson.toJson(entities), type));
							}
							
							String[] sids = parts[1].split(",");
							relations.update(new BasicDBObject("_id",  parts[0]), new BasicDBObject("$addToSet", new BasicDBObject("sentences", new BasicDBObject("$each", sids))));
						}
					}
					reader.close();
					
					// Delete mapreduce job output
					path				= new Path(directory);
					fs.delete(path, true);
					
					/**
					 * Delete first mapreduce output
					 */
					directory	= conf.get("fs.defaultFS") + String.format("/facta-pgx/mapreduce/sentences/%s/%s", year, pid);
					path				= new Path(directory);
					fs.delete(path, true);
					
					db.getMongo().close();
					fs.close();
				}
			}
		}
	}

	private static boolean sentenceJob() throws Exception {
		Job job	= Job.getInstance(conf, String.format("store_%s_%s", year, pid));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(SentenceMap.class);
		job.setReducerClass(SentenceReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(String.format("/facta-pgx/storage/publications/%s/%s", year, pid)));
		FileOutputFormat.setOutputPath(job, new Path(String.format("/facta-pgx/mapreduce/sentences/%s/%s", year, pid)));
		
		job.setJarByClass(FactapgxStorage.class);
		
		return job.waitForCompletion(true);
	}
	
	private static boolean entityJob() throws Exception {
		Job job	= Job.getInstance(conf, String.format("parse_%s_%s", year, pid));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(EntityMap.class);
		job.setReducerClass(EntityReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(String.format("/facta-pgx/mapreduce/sentences/%s/%s/part-r-00000", year, pid)));
		FileOutputFormat.setOutputPath(job, new Path(String.format("/facta-pgx/mapreduce/entities/%s/%s", year, pid)));
		
		job.setJarByClass(FactapgxStorage.class);
		
		return job.waitForCompletion(true);
	}
	
	private static boolean relationsJob() throws Exception {
		Job job	= Job.getInstance(conf, String.format("relations_%s_%s", year, pid));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(RelationMap.class);
		job.setReducerClass(RelationReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(String.format("/facta-pgx/mapreduce/entities/%s/%s/part-r-00000", year, pid)));
		FileOutputFormat.setOutputPath(job, new Path(String.format("/facta-pgx/mapreduce/relations/%s/%s", year, pid)));
		
		job.setJarByClass(FactapgxStorage.class);
		
		return job.waitForCompletion(true);
	}
}