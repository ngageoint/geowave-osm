package mil.nga.giat.osm.mapreduce.Convert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.osm.osmfeature.OSMFeatureBuilder;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;

public class OldOSMConversionMapper
		extends Mapper<Key, Value, Text, Text> {
	
	private static final Logger log = Logger.getLogger(OldOSMConversionMapper.class);
	
	private BasicAccumuloOperations _bao;
	private AccumuloDataStore _geoWaveDataStore;
	private String _user;
	private String _pass;
	private String _zookeepers;
	private String _namespace;
	private String _instance;
	private String _inputTable;
	private String _qualifier;
	private Index _index = IndexType.SPATIAL_RASTER.createDefaultIndex();
	private ZooKeeperInstance _zoo;
	private Connector _conn;
	
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);
		_user = context.getConfiguration().get("user");
		_pass = context.getConfiguration().get("pass");
		_zookeepers = context.getConfiguration().get("zookeepers");
		_namespace = context.getConfiguration().get("namespace");
		_instance = context.getConfiguration().get("instance");
		_inputTable = context.getConfiguration().get("inputTable");
		_qualifier = context.getConfiguration().get("qualifier");
		
	 	
  	  _zoo = new ZooKeeperInstance(_instance, _zookeepers);
  	  try {
			_conn = _zoo.getConnector(_user, new PasswordToken(_pass));
		} catch (AccumuloException | AccumuloSecurityException e) {
			throw new IOException("Unable to connecto to accumulo: " + e.getMessage());
		}
		
		try {
			_bao = new BasicAccumuloOperations(_zookeepers, _instance, _user, _pass, _namespace);
			
		} catch (AccumuloSecurityException | AccumuloException ex){
			log.error(ex.getMessage());
			throw new IOException("Unable to initialize datastore: " + ex.getMessage());
		}
		
		//_geoWaveDataStore = new AccumuloDataStore(new AccumuloIndexStore(_bao),	new AccumuloAdapterStore(_bao),	_bao);
	};
	
	@Override
	protected void cleanup(Mapper<Key,Value,Text,Text>.Context context) throws IOException ,InterruptedException {
		super.cleanup(context);
	};
	
	public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
 	    

    	
    	  Map<String, String> info = new HashMap<>();
    	  Map<String, String> main = new HashMap<String, String>();
    	  Map<String, String> tags = new HashMap<String, String>();
    	  Map<Integer, Map<String, String>> roles = new HashMap<Integer, Map<String, String>>();
     	  for (Entry<Key,Value> entry2 : WholeRowIterator.decodeRow(row, data).entrySet()){
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
    		  List<SimpleFeature> feats = OSMFeatureBuilder.GetFeatures(_conn, _inputTable, _qualifier, tags, main, info, roles);
    		  if (feats != null){
	    		  for (SimpleFeature sf : feats){
	    			  if (sf != null) {
	    				  _geoWaveDataStore.ingest(new FeatureDataAdapter(sf.getFeatureType()),_index, sf);
	    			  }
	    		  }
    		  }
    	  
	}

}

