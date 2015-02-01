package mil.nga.giat.osm.mapreduce;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



public class OSMPBFRunner  extends Configured implements Tool {
	private static final Logger log = Logger.getLogger(OSMPBFRunner.class);
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OSMPBFRunner(), args);
		System.exit(res);
	}
	
	public static SortedSet<Text> getRanges(){
        SortedSet<Text> splits = new TreeSet<Text>();
        
        for (String p : new String[] {"n", "w", "r"}){
	        for (int i = 0; i <= 9; i ++){
	        	splits.add(new Text(p + "_" + String.valueOf(i)));
	        }
        }
        return splits;
	}
	

	@Override
	public int run(String[] args) throws Exception {
		
		String user = "root";
		String pass = "geowave";
		String instance = "geowave";
		String zookeepers = "master.:2181";
		String tableName = "osm_virginia";
		String inputSequence = "/osm/stage2";
		
		Configuration conf = this.getConf();
        
		//job settings
		Job job = Job.getInstance(conf,"OSM Sequence Import");
		job.setJarByClass(OSMPBFRunner.class);
        
        
		//input format
		SequenceFileInputFormat.setInputPaths(job, inputSequence);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        
        
        //mappper
        job.setMapperClass(OSMPBFMapper.class);
     
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
        AccumuloOutputFormat.setCreateTables(job,true);
        AccumuloOutputFormat.setDefaultTableName(job, tableName);
        AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
        
        //set splits and locality groups
        ZooKeeperInstance inst = new ZooKeeperInstance(instance, zookeepers, 500);
        Connector conn = inst.getConnector(user, new PasswordToken(pass));
        

        
        if (!conn.tableOperations().exists(tableName)){
        	conn.tableOperations().create(tableName);
        }
        
        
        conn.tableOperations().addSplits(tableName, getRanges());
        Map<String, Set<Text>> localityGroups = new HashMap<String, Set<Text>>();
        
        Set<Text> nodetagColumns = new HashSet<Text>() ;
        nodetagColumns.add(OSMPBFMapper.nodeTagText);
        
        Set<Text> waytagColumns = new HashSet<Text>();
        waytagColumns.add(OSMPBFMapper.wayTagText);
        
        localityGroups.put("nodetags", nodetagColumns);
        localityGroups.put("waytags", waytagColumns);
        
        conn.tableOperations().setLocalityGroups(tableName, localityGroups);
        
        
        
        
        
        
        
        //reducer
        job.setNumReduceTasks(0);
        
        return job.waitForCompletion(true) ? 0 : -1;
	}

}
