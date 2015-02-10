package mil.nga.giat.osm.osmfeature;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.store.GeometryUtils;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;

public class OSMFeatureBuilder {
	
	private static final Logger log = Logger.getLogger(OSMFeatureBuilder.class);
	
	private static final List<Range> _ranges = new ArrayList<Range>();
	
	public static List<Range> getRanges(){
		/*
		if (_ranges.size() == 0){
			for (Text t : OSMPBFRunner.getRanges()){
				_ranges.add(new Range(t));
			}
		}
		return _ranges;
		*/
		return null;
	}
	
	
	public static List<SimpleFeature> GetFeatures(Connector conn, String table, String qualifier, Map<String, String> tags, Map<String, String> node, Map<String, String> info){
		return GetFeatures(conn, table, qualifier, tags, node, info, null);
	}
	
	
	public static List<SimpleFeature> GetFeatures(Connector conn, String table, String qualifier, Map<String, String> tags, Map<String, String> node, Map<String, String> info, Map<Integer, Map<String, String>> roles){
		
		OSMFeatures.FEATURES main = null;
		
		for (Entry<String,String> kvp : tags.entrySet()){
			main = OSMFeatures.FEATURES.fromString(kvp.getKey().split("__")[0]);
			if (main != null)
				break;
		}
		
		if (main == null) 
			return null;
		
		
		
		Geometry geom =  null;
		
		try {
				
			geom =	getGeometry(conn, table, qualifier, tags, node, info, roles);
		} catch (Exception ex){
			//log.error(ex.getMessage());
			ex.printStackTrace();
		}
		if (geom == null)
			return null;
		
		//SimpleFeatureType sft = OSMFeatures.GetFeature(main, geom.getGeometryType());
		SimpleFeatureType sft = OSMFeatures.GetFeatureGeom(geom.getGeometryType());
		
		SimpleFeatureBuilder b = new SimpleFeatureBuilder(sft);
		
		Long osm_id = Long.parseLong(node.get("id"));

		//requried fields
		b.add(geom);
		b.add(osm_id);
		b.add(main.getText());
		
		for (Entry<String, String> t : tags.entrySet()){
			if (t.getKey().split("__")[0].equals(main.getText())){
				b.add(t.getValue());
				break;
			}
		}
		
		
		//for (AttributeDescriptor ad : sft.getAttributeDescriptors()){
		for (int i = 4; i <sft.getAttributeCount(); i++){
			Class type = sft.getType(i).getBinding();//ad.getType().getBinding();
			String name = sft.getType(i).getName().getLocalPart();//ad.getType().getName().getLocalPart();
			for (Entry<String,String> t : tags.entrySet()){
				String key = t.getKey().split("__")[0].replace(":",  "-");
				if (key.equals(name)){
					if (t.getValue() == null || t.getValue().isEmpty()){
						//b.set(i, null);
					} else {
						try {
							b.set(i,getAttributeValue(type, t.getValue().toString()));
						}
						catch (Exception ex){
							log.error(ex.getMessage());
							ex.printStackTrace();
						}
					}
				}
			}
		}
		
		SimpleFeature mFeat = b.buildFeature(String.valueOf(osm_id) + "_" + qualifier);
		if (mFeat == null)
			System.out.println("yo");
		List<SimpleFeature> features = new ArrayList<SimpleFeature>();
		features.add(mFeat);
		return features;
	}
	
	
	private static Object getAttributeValue(Class type, String value){
		if (value == null || value.isEmpty())
			return null;
		
		if (type == String.class){
			return value;
		} else if (type == Double.class) {
			return Double.parseDouble(value);
		} else if (type == Float.class) {
			return Float.parseFloat(value);
		} else if (type == Integer.class) {
			return Integer.parseInt(value);
		} else if (type == Long.class) {
			return Long.parseLong(value);
		} else if (type == Boolean.class) {
			switch (value.toLowerCase()){
				case "0" :
					return false;
				case "1" : 
					return true;
				case "true" :
					return true;
				case "false" :
					return false;
				case "t" :
					return true;
				case "f" :
					return false;
			} 
		} else if (type == Date.class){
			return new Date(Long.parseLong(value));
		}
		
		return null;
	}
	
	public static Geometry getGeometry(Connector conn, String tableName, String qualifier, Map<String, String> tags, Map<String, String> node, Map<String, String> info, Map<Integer, Map<String, String>> roles){
		
		for (String key : tags.keySet()){
			if (qualifier.equals("n")){
				return handleNode(tags, node, info);
			} else if (qualifier.equals("w")){
				return handleWay(conn, tableName, tags, node, info);
				//return getLineorAreaGeometry(scanner, qualifier, tags, node, info);
			} else if (qualifier.equals("r")){
				return handleRelation(conn, tableName, tags, node, info, roles);
			}
		}
		return null;
	}
	
	private static Geometry handleNode(Map<String, String> tags, Map<String, String> node, Map<String, String> info){
		
			double lat, lon;
			lat = Double.parseDouble(node.get("lat"));
			lon = Double.parseDouble(node.get("lon"));
			return (Geometry)GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat));
		
	}
	
	private static boolean closedWayIsAreaExceptions(Map<String, String> tags){
		String[] nonArea = new String[] {"barrier", "highway"};
		
		for (Entry<String, String> kvp : tags.entrySet()){
			String k = kvp.getKey().split("__")[0];
			for (String s : nonArea){
				if (s.equals(k))
					return true;
			}
		}
		
		return false;
	}
	
	private static Geometry handleWay(Connector conn, String tableName, Map<String, String> tags, Map<String, String> way, Map<String, String> info){

		String refs = "";//way.get(OSMMapperBase.wayrefText.toString());

		List<Long> allRefs = new ArrayList<Long>();
		for (String l : refs.split(",")){
			allRefs.add(Long.parseLong(l));
		}

		
		if (allRefs.size() == 0)
			return null;
		List<Coordinate> pts = getPoints(allRefs, conn, tableName);

		if (pts == null || pts.size() == 0)
			return null;
		
		boolean firstAndLastSame = pts.get(0).equals2D(pts.get(pts.size() - 1));
		
		boolean area = false;
		//String areaTag = tags.get("area");
		for (Entry<String,String> s : tags.entrySet()){
			if (s.getKey().split("__")[0].equals("area")){
				if (s.getValue().equals("yes")){
					area = true;
				}
			}
		}
		
		
		
		/*
		if (areaTag != null && (areaTag.equals("yes") || areaTag.equals("!"))){
			area = true;
		}
		*/
		if (pts.size() == 1) {
			//invalid per spec, but some ways with 1 point - let's at least represent them
			return GeometryUtils.GEOMETRY_FACTORY.createPoint(pts.get(0));
		}
		else if (!area && !firstAndLastSame){
			//open polyline
			//System.out.println("open");
			return GeometryUtils.GEOMETRY_FACTORY.createLineString(pts.toArray(new Coordinate[pts.size()]));
		} else if (area || (firstAndLastSame && !closedWayIsAreaExceptions(tags))){
			//polygon / area
			//System.out.println("polygon");
			//List<Coordinate> pts2 = getPoints(allRefs, conn, tableName);
			if (area && !firstAndLastSame){
				//fix bad geometries
				pts.add(pts.get(0));
			}
			if (pts.size() >= 4)
				return GeometryUtils.GEOMETRY_FACTORY.createPolygon(pts.toArray(new Coordinate[pts.size()]));
		} else {
			//closed polyline
			//System.out.println("closed");
			return GeometryUtils.GEOMETRY_FACTORY.createLineString(pts.toArray(new Coordinate[pts.size()]));
		}
		return null;
	}
	
	private static Geometry handleRelation(Connector conn, String tableName, Map<String, String> tags, Map<String, String> node, Map<String, String> info, Map<Integer, Map<String, String>> roles){
		
		String type = null;
		
		for (Entry<String, String> tag : tags.entrySet()){
			if (tag.getKey().contains("type_")){
				type = tag.getValue();
			}
		}
		
		/*
		if (type.toLowerCase().trim().equals("multipolygon")){
			
			
			LinearRing outer = null;
			List<LinearRing> inner = new ArrayList<LinearRing>();
			
			List<Polygon> polies = new ArrayList<Polygon>();
			
			for (Entry<Integer, Map<String, String>> kvp : roles.entrySet()){
				switch (kvp.getValue().get("type")){
					case "WAY" :

						//Object[] ret = 
						
						List<Coordinate> way = getWay(Long.parseLong(kvp.getValue().get("memid")), conn, tableName);
						if (way == null || way.size() == 0){
							return null;
						} else {
							if (!way.get(0).equals2D(way.get(way.size()- 1))){
								way.add(way.get(0));
							}
						}
						if (way.size() >= 4){
							if (kvp.getValue().get("rolessid").equals("inner")){
								LinearRing lr = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(way.toArray(new Coordinate[way.size()]));
								inner.add(lr);
							} else if (kvp.getValue().get("rolessid").equals("outer")){
								if (outer == null){
									outer = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(way.toArray(new Coordinate[way.size()]));
								} else {
									polies.add(GeometryUtils.GEOMETRY_FACTORY.createPolygon(outer,inner.toArray(new LinearRing[inner.size()])));
									outer = null;
									inner.clear();
								}
							}
						}
						break;
					case "NODE" :
						System.out.println("Node not supported for multipolygon - id: " + node.get("id"));
						break;
					case "RELATION" :
						System.out.println("Relation not supported for multipolygon - id: " + node.get("id"));
						break;
				}
			}
			if (outer != null){
				polies.add(GeometryUtils.GEOMETRY_FACTORY.createPolygon(outer,inner.toArray(new LinearRing[inner.size()])));
				System.out.println("Polies size: " + polies.size());
				return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polies.toArray(new Polygon[polies.size()]));
				
			}
		}
		else*/ if (type.toLowerCase().trim().equals("route")){
			
			//LinearRing outer = null;
			//List<LineString> inner = new ArrayList<LineString>();
			
			//List<Polygon> polies = new ArrayList<Polygon>();
			
			List<LineString> mls = new ArrayList<LineString>();
			
			
			for (Entry<Integer, Map<String, String>> kvp : roles.entrySet()){
				switch (kvp.getValue().get("type")){
					case "WAY" :

						//Object[] ret = 
						LineString ls = null;
						List<Coordinate> way = getWay(Long.parseLong(kvp.getValue().get("memid")), conn, tableName);
						if (way != null)
							ls = GeometryUtils.GEOMETRY_FACTORY.createLineString(way.toArray(new Coordinate[way.size()]));
						if (ls != null)
							mls.add(ls);
						
						break;
					case "NODE" :
						System.out.println("Points not supported for multipolygon - id: " + node.get("id"));
						//break;
						//List<Long> pt = new ArrayList<Long>();
						//pt.add(Long.parseLong(kvp.getValue().get("memid")));
						
						//List<Coordinate> pts = getPoints(pt, conn, tableName);
											
					case "RELATION" :
						System.out.println("Relation not supported for multipolygon - id: " + node.get("id"));
						break;
				}
			}
			System.out.println("Route: " + mls.size());
			return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(mls.toArray((new LineString[mls.size()])));
		}else if (type.toLowerCase().trim().equals("boundary")){ 
		
		} 
		else {
			System.out.println("undefined");
		}
		
		
		return null;
	}
	
	
	
	private static List<Coordinate> getWay(Long way, Connector conn, String tableName){
		
		BatchScanner scan = null;
		List<Long> refs = new ArrayList<Long>();
		Map<String, String> tags = new HashMap<String, String>();
		try {
			 scan = conn.createBatchScanner(tableName, new Authorizations(), 1);
			
			List<Range> ranges = new ArrayList<Range>();
			
			ranges.add(new Range("w_" + way));
			
			scan.setRanges(ranges);
			//scan.fetchColumnFamily(OSMMapperBase.wayText);
			//scan.fetchColumnFamily(OSMMapperBase.wayinfoText);
			
			
			
			for (Entry<Key, Value> entry : scan ){
				if (entry.getKey().getColumnFamily().toString().equals("")){//OSMMapperBase.wayText.toString())) {
					if (entry.getKey().getColumnQualifier().toString().equals("refs")){
						String[] srefs = entry.getValue().toString().split(",");
						for (String s : srefs){
							refs.add(Long.parseLong(s));
						}
					}
				} /*else if (entry.getKey().getColumnFamily().toString().equals(OSMMapperBase.wayTagText.toString())) {
					String tagname = entry.getKey().getColumnQualifier().toString().split("__")[1];
					String val = new String(entry.getValue().get());
					tags.put(tagname, val);
				}*/
			}
			
		} catch (TableNotFoundException e) {
			log.error(e);
			return null;
		} finally {
			if (scan != null) {try {scan.close();} catch (Exception e){}}
		}
		if (refs.size() == 0)
			return null;
		return getPoints(refs, conn, tableName);
	}
	
	private static List<Coordinate> getPoints(List<Long> refs, Connector conn, String tableName){
			
			List<Coordinate> coords = new ArrayList<Coordinate>();
			BatchScanner scan = null;
		try {
			 scan = conn.createBatchScanner(tableName, new Authorizations(), 1);
			
			List<Range> ranges = new ArrayList<Range>();
			for (Long i : refs){
				ranges.add(new Range("n_" + i));
			}
			scan.setRanges(ranges);
			//scan.fetchColumnFamily(OSMMapperBase.nodeText);
			
			String key = "";
			Double lat = -1.0;
			Double lon = -1.0;
			Long id = -1l;
			Map<Long, Coordinate> allCords = new HashMap<Long, Coordinate>();
			
			for (Entry<Key, Value> entry : scan ){
				if (key.equals("")){
					key = entry.getKey().getRow().toString();
				}
				if (!entry.getKey().getRow().toString().equals(key)){
					//coords.add(new Coordinate(lon, lat));
					allCords.put(id, new Coordinate(lon, lat));
					key = entry.getKey().getRow().toString();
				} 
					
				switch (entry.getKey().getColumnQualifier().toString()){
					case "lat" : 
						lat = Double.parseDouble(new String(entry.getValue().get()));
						break;
					case "lon" :
						lon = Double.parseDouble(new String(entry.getValue().get()));
						break;
					case "id" :
						id = Long.parseLong(new String(entry.getValue().get()));
					}
				
			}
			//coords.add(new Coordinate(lon, lat));
			allCords.put(id, new Coordinate(lon, lat));
			for (long l : refs){
				coords.add(allCords.get(l));
			}
			
		} catch (TableNotFoundException e) {
			log.error(e);
			return null;
		} finally {
			if (scan != null) {try {scan.close();} catch (Exception e){}}
		}
		
		
		return coords;
		
	}

}
