package mil.nga.giat.osm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.osm.mapreduce.OSMConversionRunner;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;

public class AccTest {

    private final Logger log = Logger.getLogger(AccTest.class);
    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
    private static BasicAccumuloOperations _bao;

      public static void main(final String[] args) throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    	  
    	  Index defIndex = IndexType.SPATIAL_RASTER.createDefaultIndex();
    	  
    	  
    	  String zoo = "master.:2181";
    	  String instance = "geowave";
    	  String user = "root";
    	  String pass = "geowave";
    	  String namespace = "test1";
    	  String table = "osm_virginia";
    	  
    	  _bao = new BasicAccumuloOperations(zoo, instance, user, pass, namespace);
    	  AccumuloDataStore ds = new AccumuloDataStore(_bao);
    	  //ds.ingest(dataWriter, index, entry);
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  ZooKeeperInstance inst = new ZooKeeperInstance("geowave", "master.:2181");
    	  Connector conn = inst.getConnector("root", new PasswordToken("geowave"));
    	  
    	  
    	  
    	  
    	  //csis = new ClientSideIteratorScanner(conn.createScanner("osm_virginia", new Authorizations()));
    	  BatchScanner csis = conn.createBatchScanner(table, new Authorizations(),1);
    	  //csis.setRanges(OSMFeatureBuilder.getRanges());
    	 // IteratorSetting si = new IteratorSetting(50, "filter", OSMConversionIterator.class);
    	  String qualifier = "r";
    	  //csis.setRanges(OSMConversionRunner.getRanges("r"));
    	  /*
    	  si.addOption("cq", "relationroles");
    	  csis.addScanIterator(si);
    	  for (Entry<Key, Value> entry :  csis){
    		  Map<String, String> info = new HashMap<String, String>();
    		  Map<String, String> main = new HashMap<String, String>();
    		  Map<String, String> tags = new HashMap<String, String>();
    		  Map<Integer, Map<String, String>> roles = new HashMap<Integer, Map<String, String>>();
    		  
	  
    		  
    		  
    		  for (Entry<Key,Value> entry2 : OSMConversionIterator.decodeRow(entry.getKey(),entry.getValue()).entrySet()){
    			  String cf = entry2.getKey().getColumnFamily().toString();
    			  String cq = entry2.getKey().getColumnQualifier().toString();
    			  String v = new String(entry2.getValue().get());
    			  
	    			  if (cf.contains("tags")){
	    				  tags.put(cq, v);
	    			  } else if (cf.contains("info")) {
	    				  info.put(cq, v);
	    			  } else {
	    				  main.put(cq, v);
	    			  }
    			   if (cf.contains("roles")){
    				  Integer id = Integer.parseInt(cq.split("_")[1]);
    				  if (!roles.containsKey(id)){
    					  roles.put(id, new HashMap<String, String>());
    				  }
    				  String role = cq.split("_")[0].replace("relation","");
    				  roles.get(id).put(role, v);
    			  }
    		  }
    		  List<SimpleFeature> feats = OSMFeatureBuilder.GetFeatures(conn, "osm_virginia", qualifier, tags, main, info, roles);
    		  if (feats != null){
	    		  for (SimpleFeature sf : feats){
	    			  if (sf != null) {
	    				  ds.ingest(new FeatureDataAdapter(sf.getFeatureType()), defIndex, sf);
	    			  }
	    		  }
    		  }
    	  }
    	  csis.close();
    	  */
    	  
    	  
    	  /*
    	  
    	  
    	  
    	  //csis = new ClientSideIteratorScanner(conn.createScanner("osm_virginia", new Authorizations()));
    	  csis = conn.createBatchScanner(table, new Authorizations(),1);
    	  //csis.setRanges(OSMFeatureBuilder.getRanges());
    	  si = new IteratorSetting(50, "filter", OSMConversionIterator.class);
    	  qualifier = "n";
    	  si.addOption("cq", "nodetags");
    	  csis.addScanIterator(si);
    	  csis.setRanges(OSMConversionRunner.getRanges("n"));
    	  for (Entry<Key, Value> entry :  csis){
    		  Map<String, String> info = new HashMap<String, String>();
    		  Map<String, String> main = new HashMap<String, String>();
    		  Map<String, String> tags = new HashMap<String, String>();
    		  
    		  for (Entry<Key,Value> entry2 : OSMConversionIterator.decodeRow(entry.getKey(),entry.getValue()).entrySet()){
    			  String cf = entry2.getKey().getColumnFamily().toString();
    			  String cq = entry2.getKey().getColumnQualifier().toString();
    			  String v = new String(entry2.getValue().get());
    			  if (cf.contains("tags")){
    				  tags.put(cq, v);
    			  } else if (cf.contains("info")) {
    				  info.put(cq, v);
    			  } else {
    				  main.put(cq, v);
    			  }
    		  }
    		  List<SimpleFeature> feats = OSMFeatureBuilder.GetFeatures(conn, "osm_virginia", qualifier, tags, main, info);
    		  if (feats != null){
	    		  for (SimpleFeature sf : feats){
	    			  if (sf != null) {
	    				  ds.ingest(new FeatureDataAdapter(sf.getFeatureType()), defIndex, sf);
	    			  }
	    		  }
    		  }
    	  }
    	  csis.close();
    	  
    	  
    	  
    	  
    	  */
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  //ClientSideIteratorScanner csis = new ClientSideIteratorScanner(conn.createScanner("osm_virginia", new Authorizations()));
    	   csis = conn.createBatchScanner(table, new Authorizations(),1);
    	  //csis.setRanges(OSMFeatureBuilder.getRanges());
//    	   si = new IteratorSetting(50, "filter", OSMConversionIterator.class);
    	   qualifier = "w";
  //  	  si.addOption("cq", "waytags");
    	  //csis.addScanIterator(si);
    	  csis.setRanges(OSMConversionRunner.getRanges("w"));
    	  for (Entry<Key, Value> entry :  csis){
    		  Map<String, String> info = new HashMap<String, String>();
    		  Map<String, String> main = new HashMap<String, String>();
    		  Map<String, String> tags = new HashMap<String, String>();
    		  
    		  for (Entry<Key,Value> entry2 : WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()).entrySet()){
    			  String cf = entry2.getKey().getColumnFamily().toString();
    			  String cq = entry2.getKey().getColumnQualifier().toString();
    			  String v = new String(entry2.getValue().get());
    			  if (cf.contains("tags")){
    				  tags.put(cq, v);
    			  } else if (cf.contains("info")) {
    				  info.put(cq, v);
    			  } else {
    				  main.put(cq, v);
    			  }
    		  }
    		  List<SimpleFeature> feats = OSMFeatureBuilder.GetFeatures(conn, "osm_virginia", qualifier, tags, main, info);
    		  if (feats != null){
	    		  for (SimpleFeature sf : feats){
	    			  if (sf != null) {
	    				  ds.ingest(new FeatureDataAdapter(sf.getFeatureType()), defIndex, sf);
	    			  }
	    		  }
    		  }
    	  }
    	  csis.close();
    	  
    	  
    	  
    	  
    	  
    	  
    
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	  
    	 
    }
}
