package mil.nga.giat.osm.mapreduce.Convert;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class OldOSMConversionRunner
		extends Configured implements Tool {
private static final Logger log = Logger.getLogger(OldOSMConversionRunner.class);
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OldOSMConversionRunner(), args);
		System.exit(res);
	}

	public static SortedSet<Range> getRanges(String p){
        SortedSet<Range> splits = new TreeSet<Range>();


	        for (int i = 0; i <= 9; i ++){

	        	splits.add(Range.prefix(p + "_" + String.valueOf(i)));
	        }

        return splits;
	}

	
	@Override
	public int run(String[] args) throws Exception {
		
		String user = "root";
		String pass = "geowave";
		String instance = "geowave";
		String zookeepers = "master.:2181";
		String inputTable = "osm_virginia";
		String namespace = "osm_virginia";
		
		
		
		Configuration conf = this.getConf();
        
		//job settings
		
        
		conf.set("user", user);
		conf.set("pass", pass);
		conf.set("instance", instance);
		conf.set("zookeepers", zookeepers);
		conf.set("namespace", namespace);
		conf.set("inputTable", inputTable);
		conf.set("qualifier", "w");
        
		Job job = Job.getInstance(conf,"OSM Conversion - Ways");
		job.setJarByClass(OldOSMConversionRunner.class);
		  
  	  
		
		//input format
		
		AccumuloInputFormat.setConnectorInfo(job,user, new PasswordToken(pass));
		AccumuloInputFormat.setInputTableName(job, inputTable);
		AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers);
		AccumuloInputFormat.setScanAuthorizations(job,new Authorizations());
		//IteratorSetting is = new IteratorSetting(50, "filter", OSMConversionIterator.class);
		//is.addOption("cq", "waytags");
		//AccumuloInputFormat.addIterator(job, is);
		
		  
		
		
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		AccumuloInputFormat.setRanges(job, getRanges("w"));
        
        //mappper
		
        job.setMapperClass(OldOSMConversionMapper.class);
        
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        
       // TextOutputFormat.setOutputPath(job, new Path(outfile));
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Mutation.class);
        //job.setOutputFormatClass(AccumuloOutputFormat.class);
        //AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
        //AccumuloOutputFormat.setCreateTables(job,true);
        //AccumuloOutputFormat.setDefaultTableName(job, tableName);
        //AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
        
        
        //reducer
        job.setNumReduceTasks(0);
        
        if (!job.waitForCompletion(true)){
        	return -1;
        }
    	
        conf.set("qualifier", "n");
		job = Job.getInstance(conf,"OSM Conversion - Nodes");
		job.setJarByClass(OldOSMConversionRunner.class);
	
		
		AccumuloInputFormat.setConnectorInfo(job,user, new PasswordToken(pass));
		AccumuloInputFormat.setInputTableName(job, inputTable);
		AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers);
		AccumuloInputFormat.setScanAuthorizations(job,new Authorizations());
//		is = new IteratorSetting(50, "filter", OSMConversionIterator.class);
//		is.addOption("cq", "nodetags");
		//AccumuloInputFormat.addIterator(job, is);
		
		  
		
		
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		AccumuloInputFormat.setRanges(job, getRanges("n"));
        
        //mappper
		
        job.setMapperClass(OldOSMConversionMapper.class);
        
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        
       // TextOutputFormat.setOutputPath(job, new Path(outfile));
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Mutation.class);
        //job.setOutputFormatClass(AccumuloOutputFormat.class);
        //AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
        //AccumuloOutputFormat.setCreateTables(job,true);
        //AccumuloOutputFormat.setDefaultTableName(job, tableName);
        //AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
        
        
        //reducer
        job.setNumReduceTasks(0);
        
        if (!job.waitForCompletion(true)){
        	return -1;
        }
        
        conf.set("qualifier", "r");
		job = Job.getInstance(conf,"OSM Conversion - Relations");
		job.setJarByClass(OldOSMConversionRunner.class);
        
        AccumuloInputFormat.setConnectorInfo(job,user, new PasswordToken(pass));
		AccumuloInputFormat.setInputTableName(job, inputTable);
		AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers);
		AccumuloInputFormat.setScanAuthorizations(job,new Authorizations());
//		is = new IteratorSetting(50, "filter", OSMConversionIterator.class);
//		is.addOption("cq", "relationroles");
//		AccumuloInputFormat.addIterator(job, is);

	 	
		  
		
		
		job.setInputFormatClass(AccumuloInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		AccumuloInputFormat.setRanges(job, getRanges("r"));
        
        //mappper
		
        job.setMapperClass(OldOSMConversionMapper.class);
        
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        
       // TextOutputFormat.setOutputPath(job, new Path(outfile));
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Mutation.class);
        //job.setOutputFormatClass(AccumuloOutputFormat.class);
        //AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
        //AccumuloOutputFormat.setCreateTables(job,true);
        //AccumuloOutputFormat.setDefaultTableName(job, tableName);
        //AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
        
        
        //reducer
        job.setNumReduceTasks(0);
        

        return job.waitForCompletion(true) ? 0 : -1;
	}

}
