package mil.nga.giat.osm.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.InflaterInputStream;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.Fileformat.Blob;
import org.openstreetmap.osmosis.osmbinary.Osmformat.DenseNodes;
import org.openstreetmap.osmosis.osmbinary.Osmformat.PrimitiveBlock;
import org.openstreetmap.osmosis.osmbinary.Osmformat.PrimitiveGroup;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Node;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Relation;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Relation.MemberType;
import org.openstreetmap.osmosis.osmbinary.Osmformat.StringTable;
import org.openstreetmap.osmosis.osmbinary.Osmformat.Way;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;






public class OSMPBFMapper extends Mapper<LongWritable, BytesWritable, Text, Mutation>{

	private static final Logger log = Logger.getLogger(OSMPBFMapper.class);
	private static final HashFunction hf = Hashing.murmur3_128(1);
	private static final Charset defaultCharset = Charsets.UTF_8;
	
	



	//column families
	public static final Text nodeText = new Text("node");
	public static final Text nodeinfoText = new Text("nodeinfo");
	public static final Text nodeTagText = new Text("nodetags");
	public static final Text wayText = new Text("way");
	public static final Text wayTagText = new Text("waytags");
	public static final Text wayinfoText = new Text("wayinfo");
	public static final Text relationText = new Text("relation");
	public static final Text relationinfoText = new Text("relationinfo");
	public static final Text relationTagText = new Text("relationtags");
	
	
	private Text tableName = new Text("osm_virginia");
	private static final Text empty = new Text("empty");
	
	
	private static final Text relationrolesText = new Text("relationroles");
	
	private static final Text relationrolesdsidText = new Text("relationrolessid");
	private static final Text relationmemidsText = new Text("relationmemid");
	private static final Text relationtypesText = new Text("relationtype");
	
	
	//nodes
	private static final Text idText = new Text("id");
	private static final Text latText = new Text("lat");
	private static final Text lonText = new Text("lon");
	
	//info
	private static final Text versionText  = new Text("version");
	private static final Text timestampText = new Text("timestamp");
	private static final Text changesetText = new Text("changeset");
	private static final Text uidText = new Text("uid");
	private static final Text usersidText= new Text("user_sid");
	private static final Text visibilityText= new Text("visibility");
	
	
	//ways
	private static final Text wayId = new Text("id");
	public static final Text wayrefText = new Text("refs");
	
	//relations
	private static final Text relationId = new Text("id");
	
	public static void main(String args[]){
		System.out.println("yo");
		
	}
	
	
	
	public void SetTableName(String table){
		tableName = new Text(table);
	}
	
	private static byte[] longToBytes(long val){
		byte[] a = new byte[8];
		a[0] = (byte)(val >>> 56);
	    a[1] = (byte)(val >>> 48);
	    a[2] = (byte)(val >>> 40);
	    a[3] = (byte)(val >>> 32);
	    a[4] = (byte)(val >>> 24);
	    a[5] = (byte)(val >>> 16);
	    a[6] = (byte)(val >>>  8);
	    a[7] = (byte)val;
		return a;
	}
	
	private static byte[] intToBytes(int val){
		byte[] a = new byte[4];
		
	    a[0] = (byte)(val >>> 24);
	    a[1] = (byte)(val >>> 16);
	    a[2] = (byte)(val >>>  8);
	    a[3] = (byte)val;
		return a;
	}
	
	private static void put(Mutation m, Text cf, Text cq, long val){

		//m.put(cf, cq, new Value(longToBytes(val)));
		m.put(cf, cq, new Value(String.valueOf(val).getBytes()));
	}
	
	private static void put(Mutation m, Text cf, Text cq, int val){

		//m.put(cf, cq, new Value(intToBytes(val)));
		m.put(cf, cq, new Value(String.valueOf(val).getBytes()));
	}
	
	private static void put(Mutation m, Text cf, Text cq, double val){

		//m.put(cf, cq, new Value(intToBytes(val)));
		m.put(cf, cq, new Value(String.valueOf(val).getBytes()));
	}
	
	private static void put(Mutation m, Text cf, Text cq, String val){

		//m.put(cf, cq, new Value(val.getBytes(defaultCharset)));
		m.put(cf, cq, new Value(val.getBytes()));
	}
	
	private static void put(Mutation m, Text cf, Text cq, boolean val){

		//m.put(cf, cq, new Value(val.getBytes(defaultCharset)));
		m.put(cf, cq, new Value(val ? "1".getBytes() : "0".getBytes()));

	}
	
    private static double parseLat(long degree, int granularity, long lat_offset) {
      return  .000000001 * (granularity * degree + lat_offset);
    }

    private static double parseLon(long degree, int granularity, long lon_offset) {
       return   .000000001 * (granularity * degree + lon_offset);
    }
    
    private static long parseTimestamp(long timestamp, int granularity) {
    	return timestamp * granularity;
    }
    
    private static String getString(int id, StringTable table){
    	return table.getS(id).toStringUtf8();
    }
    
   // public static String getChar(int i){
   // 	return i >= 0 && i < 26 ? String.valueOf((char)(i + 'a')) : "-error";
  //  }
    
  //  public static String getRowKey(long key){
//		return getChar((int)(key % 26)) + "_" + key;
//	}
    
    public static long getIdFromRowKey(String rowkey){
    	return Long.parseLong(rowkey.split("_")[1]);
    }
    
    
	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException{

		Blob blob = Blob.parseFrom(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()));
	 	
		
		InputStream blobData;
		if (blob.hasZlibData()){
			blobData = new InflaterInputStream(blob.getZlibData().newInput());
		} else {
			blobData = blob.getRaw().newInput();
		}
		
		PrimitiveBlock pb = PrimitiveBlock.parseFrom(blobData);
		blobData.close();
		
		  for (PrimitiveGroup pg : pb.getPrimitivegroupList()) {
			  
			   // Here's comments on visibility from spec
			   // The visible flag is used to store history information. It indicates that
			   // the current object version has been created by a delete operation on the
			   // OSM API.
			   // When a writer sets this flag, it MUST add a required_features tag with
			   // value "HistoricalInformation" to the HeaderBlock.
			   // If this flag is not available for some object it MUST be assumed to be
			   // true if the file has the required_features tag "HistoricalInformation"
			   // set.
			  
			  
			  
			  for (Node node : pg.getNodesList()){
				  Mutation m = new Mutation("n_" + node.getId());
				  
				  put(m, nodeText, idText,node.getId());
				  put(m, nodeText, latText,parseLat(node.getLat(), pb.getGranularity(),pb.getLatOffset()));
				  put(m, nodeText, lonText ,parseLon(node.getLon(), pb.getGranularity(), pb.getLonOffset()));
				  
				  if (node.getInfo() != null){
					  put(m, nodeinfoText, versionText, node.getInfo().getVersion());
					  put(m, nodeinfoText, timestampText, parseTimestamp(node.getInfo().getTimestamp(),pb.getDateGranularity()));
					  put(m, nodeinfoText, changesetText, node.getInfo().getChangeset());
					  put(m, nodeinfoText, uidText, node.getInfo().getUid());
					  put(m, nodeinfoText, usersidText, getString(node.getInfo().getUserSid(), pb.getStringtable()));
					  
					  if (node.getInfo().hasVisible()){
						  put(m, nodeinfoText, visibilityText, node.getInfo().getVisible());
					  } else {
						  put(m, nodeinfoText, visibilityText, true);
					  }
				  }
				  
				  
				for (int k = 0; k < node.getKeysCount(); k++){
	        		String keyString = getString(node.getKeys(k), pb.getStringtable());
	        		String keyValue = getString(node.getVals(k), pb.getStringtable());
	        		put(m, nodeTagText, new Text(keyString + "__" + k), keyValue);
	        	}
				  
				  
				  context.write(tableName, m);
			  }
			  
			  
			  
			  
			  for (Way way : pg.getWaysList()){
				    HashCode hc = hf.hashLong(way.getId());
	        		Mutation m = new Mutation("w_" + way.getId());
			
	        		put (m, wayText, wayId, way.getId());
	        		
	        		
	        		
	        		List<String> allRefs = new ArrayList<String>();
	        		long lastRef = 0;
	        		for ( long r : way.getRefsList()){
	        			lastRef += r;
	        			log.warn(lastRef);
	        			allRefs.add(String.valueOf(lastRef));
	        		}
	        		put(m, wayText, wayrefText, StringUtils.join(allRefs, ","));
	        		
	        		for (int k = 0; k < way.getKeysCount(); k++){
	        			String keyString = getString(way.getKeys(k), pb.getStringtable());
	        			String keyValue = getString(way.getVals(k), pb.getStringtable());
	        			put(m, wayTagText, new Text(keyString + "__" + k), keyValue);
	        		}
	        		
	        		if (way.getInfo() != null){
	        			put(m, wayinfoText, versionText, way.getInfo().getVersion());
	        			put(m, wayinfoText, timestampText, way.getInfo().getTimestamp());
	        			put(m, wayinfoText, changesetText, way.getInfo().getChangeset());
	        			put(m, wayinfoText, uidText, way.getInfo().getUid());
	        			put(m, wayinfoText, usersidText, getString(way.getInfo().getUserSid(), pb.getStringtable()));
	        			
	        			if (way.getInfo().hasVisible()){
							  put(m, wayinfoText, visibilityText, way.getInfo().getVisible());
						  } else {
							  put(m, wayinfoText, visibilityText, true);
						  }
					}
	        		
	        		context.write(tableName, m);
			  }
			  
			  
			  
			  
			  
			  for (Relation rel : pg.getRelationsList()) {
				
				  long lastMemid = 0;
	        	  Mutation m = new Mutation("r_" + rel.getId());
				  
	        	  put (m, relationText, relationId, rel.getId());
	        	  
	        		if (rel.getInfo() != null){
	        			put(m, relationinfoText, versionText, rel.getInfo().getVersion());
						put(m, relationinfoText, timestampText, rel.getInfo().getTimestamp());
						put(m, relationinfoText, changesetText, rel.getInfo().getChangeset());
						put(m, relationinfoText, uidText, rel.getInfo().getUid());
						put(m, relationinfoText, usersidText, getString(rel.getInfo().getUserSid(), pb.getStringtable()));
						
						if (rel.getInfo().hasVisible()){
							  put(m, wayinfoText, visibilityText, rel.getInfo().getVisible());
						  } else {
							  put(m, wayinfoText, visibilityText, true);
						  }
					}
	        		for (int k = 0; k < rel.getKeysCount(); k++){
	        			String keyString = getString(rel.getKeys(k), pb.getStringtable());
	        			String keyValue = getString(rel.getVals(k), pb.getStringtable());
	        			put(m, relationTagText, new Text(keyString + "__" + k), keyValue);
	        		}
	        		
	        		
	        		
	        		//parallel arrays for rols/memids/types
	        		for (int i = 0; i < rel.getRolesSidCount(); i++){
	        			lastMemid += rel.getMemids(i);
	        			put (m, relationrolesText, new Text(relationrolesdsidText.toString() + "_" + i), getString(rel.getRolesSid(i), pb.getStringtable()));
	        			put (m, relationrolesText, new Text(relationmemidsText.toString() + "_" + i), lastMemid);
	        			String memberType = null;
	        			MemberType mt = rel.getTypes(i);
	        			if (mt.getNumber() == 0) {
	        				memberType = "NODE";
	        			} else if (mt.getNumber() == 1){
	        				memberType = "WAY";
	        			} else {
	        				memberType = "RELATION";
	        			}
	        			put (m, relationrolesText, new Text(relationtypesText + "_" + i), memberType);
	        		}
	        	  context.write(tableName, m);
			  }
			  
			  if (pg.hasDense()){
				  
				  DenseNodes dn = pg.getDense();
				  
				  long lastId = 0;
				  long lastLat =0;
				  long lastLon = 0;
				  long lastTimestamp = 0;
				  long lastChangeset = 0;
				  int lastUid = 0;
				  int lastSid = 0;
				  
				  int tagLocation = 0;
				  
				  boolean hasVisibility = dn.getDenseinfo().getVisibleList() != null && dn.getDenseinfo().getVisibleList().size() > 0;
				  
				  for (int i = 0; i < dn.getIdCount(); i++){
					  
					  
					  //it's all relative encoded
					  lastId += dn.getId(i);
		              lastLat += dn.getLat(i);
		              lastLon += dn.getLon(i);
					  
		              Mutation m = new Mutation("n_" + lastId);
					  
					  put(m, nodeText, idText, lastId);
					  put(m, nodeText, latText, parseLat(lastLat, pb.getGranularity(), pb.getLatOffset()));
					  put(m, nodeText, lonText , parseLon(lastLon, pb.getGranularity(), pb.getLonOffset()));
					  
					  //Weird spec - keys and values are mashed sequentially, and end of data for a particular node is denoted by a value of 0
					  if (dn.getKeysValsCount() > 0){
						  while (dn.getKeysVals(tagLocation) != 0){
							  String tagK = getString(dn.getKeysVals(tagLocation), pb.getStringtable());
							  tagLocation++;
							  String tagV = getString(dn.getKeysVals(tagLocation), pb.getStringtable());
							  tagLocation++;
							  put(m, nodeTagText, new Text(tagK + "__" + tagLocation), tagV);
						  }
					  }
					  
					  if (dn.getDenseinfo() != null){
						  
						  lastTimestamp += dn.getDenseinfo().getTimestamp(i);
						  lastChangeset += dn.getDenseinfo().getChangeset(i);
						  lastUid += dn.getDenseinfo().getUid(i);
						  lastSid += dn.getDenseinfo().getUserSid(i);
						  
						  put(m, nodeinfoText, versionText, dn.getDenseinfo().getVersion(i));
						  put(m, nodeinfoText, timestampText, parseTimestamp(lastTimestamp, pb.getDateGranularity()));
						  put(m, nodeinfoText, changesetText, lastChangeset);
						  put(m, nodeinfoText, uidText, lastUid);
						  put(m, nodeinfoText, usersidText, getString(lastSid, pb.getStringtable()));
						  
						  if (hasVisibility) {
							  put(m, nodeinfoText, visibilityText, dn.getDenseinfo().getVisible(i));
						  } else {
							  put(m, nodeinfoText, visibilityText, true);
						  }
					  }
					  context.write(tableName, m);
				  }
				  
			  }
	        }
		}
	}
