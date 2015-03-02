package mil.nga.giat.osm.mapreduce.Convert;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.data.field.BasicReader;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.mapreduce.Convert.OsmProvider.OsmProvider;
import mil.nga.giat.osm.osmfeature.types.attributes.AttributeDefinition;
import mil.nga.giat.osm.osmfeature.types.attributes.AttributeTypes;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinition;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.osm.osmfeature.types.features.FeatureType;
import mil.nga.giat.osm.types.TypeUtils;
import mil.nga.giat.osm.types.generated.MemberType;
import mil.nga.giat.osm.types.generated.Relation;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleFeatureGenerator
{

	private static final Logger log = LoggerFactory.getLogger(SimpleFeatureGenerator.class);



	public List<SimpleFeature> mapOSMtoSimpleFeature(Map<Key, Value> items, OsmProvider osmProvider){

		List<SimpleFeature> features = new ArrayList<>();
		OSMUnion osmunion = new OSMUnion(items);


		for (FeatureDefinition fd : FeatureDefinitionSet.Features){

			String mappingVal = null;
			String subMappingVal = null;

			boolean matched = false;
			for (String mapper : fd.MappingKeys){
				if (osmunion.tags != null) { //later handle relations where tags on on ways
					for (Map.Entry<String, String> tag : osmunion.tags.entrySet()) {
						if (tag.getKey().equals(mapper)) {
							if (fd.Mappings != null) {
								if (fd.isMappedValue(tag.getValue())) {
									matched = true;
									mappingVal = tag.getValue();
								}
							}
							else if (fd.SubMappings != null) {
								String subval = fd.getSubMappingClass(tag.getKey(), tag.getValue());
								if (subval != null) {
									subMappingVal = subval;
									matched = true;
								}
							}
						}
					}
				}
			}
			if (!matched){
				continue;
			}

			//System.out.println("Matched: " + osmunion.Id);

				// feature matches this osm entry, let's being

				SimpleFeatureType sft = FeatureDefinitionSet.featureTypes.get(fd.Name);
				SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(sft);

				for (AttributeDefinition ad : fd.Attributes ){
					if (ad.Type.equals("id")){
						sfb.set(FeatureDefinitionSet.normalizeOsmNames(ad.Name), ad.convert(osmunion.Id));
					} else if (ad.Type.equals("geometry")){
						Geometry geom = getGeometry(osmunion, osmProvider, fd);
						if (geom == null){
							return null;
						}
						sfb.set(FeatureDefinitionSet.normalizeOsmNames(ad.Name), geom);
					} else if (ad.Type.equals("mapping_value")){
						sfb.set(FeatureDefinitionSet.normalizeOsmNames(ad.Name), ad.convert(mappingVal));
					} else if (ad.Type.equals("mapping_key")){
						sfb.set(FeatureDefinitionSet.normalizeOsmNames(ad.Name), ad.convert(subMappingVal));
					} else if (ad.Key != null && !ad.Key.equals("null")) {
						if (osmunion.tags.containsKey(ad.Key)) {
							try {
								sfb.set(FeatureDefinitionSet.normalizeOsmNames(ad.Name), ad.convert(osmunion.tags.get(ad.Key)));
							} catch (Exception e) {
								System.out.println(e.toString());
							}
						}
					}
				}
			features.add(sfb.buildFeature(String.valueOf(osmunion.Id) + osmunion.OsmType.toString()));
		}


		return features;
	}


	private static Geometry getGeometry(OSMUnion osm, OsmProvider provider, FeatureDefinition fd){
		switch (osm.OsmType){
			case NODE: {
				return GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(osm.Longitude, osm.Lattitude));
			}
			case RELATION: {
				return provider.processRelation(osm, fd);
			}
			case WAY: {
				return provider.processWay(osm, fd);
			}
		}
		return null;
	}



	public static enum OSMType{
		NODE, WAY, RELATION, UNSET
	}

	public static class OSMUnion{

		private final static Logger log = LoggerFactory.getLogger(OSMUnion.class);

		protected final BasicReader.LongReader _longReader = new BasicReader.LongReader();
		protected final BasicReader.IntReader _intReader = new BasicReader.IntReader();
		protected final BasicReader.StringReader _stringReader = new BasicReader.StringReader();
		protected final BasicReader.DoubleReader _doubleReader = new BasicReader.DoubleReader();
		protected final BasicReader.BooleanReader _booleanReader = new BasicReader.BooleanReader();
		protected final BasicReader.CalendarReader _calCalendarReader = new BasicReader.CalendarReader();


		//Common
		public Long Id = null;
		public Long Version = null;
		public Long Timestamp = null;
		public Long Changeset = null;
		public Long UserId = null;
		public String UserName = null;
		public Boolean Visible = true; //per spec - default to true

		//nodes
		public Double Lattitude = null;
		public Double Longitude = null;


		//ways
		public List<Long> Nodes = null;

		//relations
		public Map<Integer, RelationSet> relationSets = null;

		public Map<String, String> tags = null;

		public OSMType OsmType = OSMType.UNSET;


		public OSMUnion(){};

		public OSMUnion(Map<Key, Value> osm){
			for (Map.Entry<Key, Value> item : osm.entrySet()){
				if (OsmType.equals(OSMType.UNSET)){
					ByteSequence CF = item.getKey().getColumnFamilyData();
					if (Schema.arraysEqual(CF, Schema.CF.NODE)){
						OsmType = OSMType.NODE;
					} else if (Schema.arraysEqual(CF, Schema.CF.WAY)){
						OsmType = OSMType.WAY;
					} else if (Schema.arraysEqual(CF, Schema.CF.RELATION)){
						OsmType = OSMType.RELATION;
					}
				}

				ByteSequence CQ = item.getKey().getColumnQualifierData();
				if (Schema.arraysEqual(CQ, Schema.CQ.ID)){
					Id = _longReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.VERSION)){
					Version = _longReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.TIMESTAMP)){
					Timestamp = _longReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.CHANGESET)){
					Changeset = _longReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.USER_ID)){
					UserId  = _longReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.USER_TEXT)){
					UserName = _stringReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.OSM_VISIBILITY)){
					Visible = _booleanReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.LATITUDE)){
					Lattitude = _doubleReader.readField(item.getValue().get());
				} else if (Schema.arraysEqual(CQ, Schema.CQ.LONGITUDE)){
					Longitude = _doubleReader.readField(item.getValue().get());
				}
				else if (Schema.arraysEqual(CQ, Schema.CQ.REFERENCES)){
					try {
						Nodes = TypeUtils.deserializeLongArray(item.getValue().get(), null).getIds();
					}
					catch (IOException e) {
						log.error("Error deserializing Avro encoded Relation member set", e);
					}
				} else if (Schema.startsWith(CQ, Schema.CQ.REFERENCE_MEMID_PREFIX.getBytes(Schema.CHARSET))){
					String s = new String(CQ.toArray());
					Integer id = Integer.valueOf(s.split("_")[1]);
					if (relationSets == null){
						relationSets = new HashMap<>();
					}
					if (!relationSets.containsKey(id)){
						relationSets.put(id, new RelationSet());
					}
					relationSets.get(id).MemId = _longReader.readField(item.getValue().get());
				} else if (Schema.startsWith(CQ, Schema.CQ.REFERENCE_ROLEID_PREFIX.getBytes(Schema.CHARSET))){
					String s = new String(CQ.toArray());
					Integer id = Integer.valueOf(s.split("_")[1]);
					if (relationSets == null){
						relationSets = new HashMap<>();
					}
					if (!relationSets.containsKey(id)){
						relationSets.put(id, new RelationSet());
					}
					relationSets.get(id).RoleId = _stringReader.readField(item.getValue().get());
				} else if (Schema.startsWith(CQ, Schema.CQ.REFERENCE_TYPE_PREFIX.getBytes(Schema.CHARSET))){
					String s = new String(CQ.toArray());
					Integer id = Integer.valueOf(s.split("_")[1]);
					if (relationSets == null){
						relationSets = new HashMap<>();
					}
					if (!relationSets.containsKey(id)){
						relationSets.put(id, new RelationSet());
					}
					switch (_stringReader.readField(item.getValue().get())){
						case "NODE" :{
							relationSets.get(id).MemType = MemberType.NODE;
							break;
						}
						case "WAY" : {
							relationSets.get(id).MemType = MemberType.WAY;
							break;
						}
						case "RELATION" : {
							relationSets.get(id).MemType = MemberType.RELATION;
							break;
						}
					}
				} else {
					// these should all be tags
					if (tags == null){
						tags = new HashMap<>();
					}
					tags.put(item.getKey().getColumnQualifier().toString(), new String(item.getValue().get()));
				}
			}


		}

		/*
		public void print(){
			if (tags == null)
				return;
			System.out.println("----------------------------------------------");
			System.out.println(String.format("ID: %d", Id));
			System.out.println(String.format("Changeset: %d", Changeset));
			System.out.println(String.format("User: %s", UserName));
			System.out.println(String.format("Num nodes: %s", Nodes == null ? "<NULL>" : Nodes.size()));
			System.out.println(String.format("Num relations: %s", relationSets == null ? "<NULL>" :relationSets.size()));
			System.out.println(String.format("Num Tags: %s", tags == null ? "<NULL>" :tags.size()));
			System.out.println("----------------------------------------------");
		}
		*/
	}



	public static class RelationSet{
		public String RoleId = null;
		public Long MemId = null;
		public MemberType MemType = null;
	}






}
