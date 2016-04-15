package mil.nga.giat.osm.mapreduce.Convert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.osm.accumulo.osmschema.ColumnFamily;
import mil.nga.giat.osm.accumulo.osmschema.ColumnQualifier;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.mapreduce.Convert.OsmProvider.OsmProvider;
import mil.nga.giat.osm.osmfeature.types.attributes.AttributeDefinition;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinition;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.osm.types.TypeUtils;
import mil.nga.giat.osm.types.generated.MemberType;

public class SimpleFeatureGenerator
{

	private static final Logger log = LoggerFactory.getLogger(
			SimpleFeatureGenerator.class);

	public List<SimpleFeature> mapOSMtoSimpleFeature(
			final Map<Key, Value> items,
			final OsmProvider osmProvider ) {

		final List<SimpleFeature> features = new ArrayList<>();
		final OSMUnion osmunion = new OSMUnion(
				items);

		for (final FeatureDefinition fd : FeatureDefinitionSet.Features) {

			String mappingKey = null;
			String mappingVal = null;

			boolean matched = false;
			for (final String mapper : fd.MappingKeys) {
				if (osmunion.tags != null) { // later handle relations where
												// tags on on ways
					for (final Map.Entry<String, String> tag : osmunion.tags.entrySet()) {
						if (tag.getKey().equals(
								mapper)) {
							if ((fd.Mappings != null) && (fd.Mappings.size() > 0)) {
								if (fd.isMappedValue(
										tag.getValue())) {
									matched = true;
									mappingVal = tag.getValue();
								}
							}
							if ((fd.SubMappings != null) && (fd.SubMappings.size() > 0)) {
								final String subval = fd.getSubMappingClass(
										tag.getKey(),
										tag.getValue());
								if (subval != null) {
									mappingKey = subval;
									mappingVal = tag.getValue();
									matched = true;
								}
							}
						}
					}
				}
			}
			if (!matched) {
				continue;
			}

			// feature matches this osm entry, let's being
			final SimpleFeatureType sft = FeatureDefinitionSet.featureTypes.get(
					fd.Name);
			final SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(
					sft);

			for (final AttributeDefinition ad : fd.Attributes) {
				if (ad.Type.equals(
						"id")) {
					sfb.set(
							FeatureDefinitionSet.normalizeOsmNames(
									ad.Name),
							ad.convert(
									osmunion.Id));
				}
				else if (ad.Type.equals(
						"geometry")
						|| ad.Type.equals(
								"validated_geometry")) {
					final Geometry geom = getGeometry(
							osmunion,
							osmProvider,
							fd);
					if (geom == null) {
						log.error(
								"Unable to generate geometry for {} of type {}",
								osmunion.Id,
								osmunion.OsmType.toString());
						return null;
					}
					sfb.set(
							FeatureDefinitionSet.normalizeOsmNames(
									ad.Name),
							geom);
				}
				else if (ad.Type.equals(
						"mapping_value")) {
					sfb.set(
							FeatureDefinitionSet.normalizeOsmNames(
									ad.Name),
							ad.convert(
									mappingVal));
				}
				else if (ad.Type.equals(
						"mapping_key")) {
					sfb.set(
							FeatureDefinitionSet.normalizeOsmNames(
									ad.Name),
							ad.convert(
									mappingKey));
				}
				else if ((ad.Key != null) && !ad.Key.equals(
						"null")) {
					if (osmunion.tags.containsKey(
							ad.Key)) {
						try {
							sfb.set(
									FeatureDefinitionSet.normalizeOsmNames(
											ad.Name),
									ad.convert(
											osmunion.tags.get(
													ad.Key)));
						}
						catch (final Exception e) {
							System.out.println(
									e.toString());
						}
					}
				}
			}
			features.add(
					sfb.buildFeature(
							String.valueOf(
									osmunion.Id) + osmunion.OsmType.toString()));
		}
		return features;
	}

	private static Geometry getGeometry(
			final OSMUnion osm,
			final OsmProvider provider,
			final FeatureDefinition fd ) {
		switch (osm.OsmType) {
			case NODE: {
				return GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								osm.Longitude,
								osm.Lattitude));
			}
			case RELATION: {
				return provider.processRelation(
						osm,
						fd);
			}
			case WAY: {
				return provider.processWay(
						osm,
						fd);
			}
		}
		return null;
	}

	public static enum OSMType {
		NODE,
		WAY,
		RELATION,
		UNSET
	}

	public static class OSMUnion
	{

		private final static Logger log = LoggerFactory.getLogger(
				OSMUnion.class);

		protected final FieldReader<Long> longReader = FieldUtils.getDefaultReaderForClass(
				Long.class);
		protected final FieldReader<Integer> intReader = FieldUtils.getDefaultReaderForClass(
				Integer.class);
		protected final FieldReader<String> stringReader = FieldUtils.getDefaultReaderForClass(
				String.class);
		protected final FieldReader<Double> doubleReader = FieldUtils.getDefaultReaderForClass(
				Double.class);
		protected final FieldReader<Boolean> booleanReader = FieldUtils.getDefaultReaderForClass(
				Boolean.class);
		protected final FieldReader<Calendar> calendarReader = FieldUtils.getDefaultReaderForClass(
				Calendar.class);

		// Common
		public Long Id = null;
		public Long Version = null;
		public Long Timestamp = null;
		public Long Changeset = null;
		public Long UserId = null;
		public String UserName = null;
		public Boolean Visible = true; // per spec - default to true

		// nodes
		public Double Lattitude = null;
		public Double Longitude = null;

		// ways
		public List<Long> Nodes = null;

		// relations
		public Map<Integer, RelationSet> relationSets = null;

		public Map<String, String> tags = null;

		public OSMType OsmType = OSMType.UNSET;

		public OSMUnion() {}

		public OSMUnion(
				final Map<Key, Value> osm ) {
			for (final Map.Entry<Key, Value> item : osm.entrySet()) {
				if (OsmType.equals(
						OSMType.UNSET)) {
					final ByteSequence CF = item.getKey().getColumnFamilyData();
					if (Schema.arraysEqual(
							CF,
							ColumnFamily.NODE)) {
						OsmType = OSMType.NODE;
					}
					else if (Schema.arraysEqual(
							CF,
							ColumnFamily.WAY)) {
						OsmType = OSMType.WAY;
					}
					else if (Schema.arraysEqual(
							CF,
							ColumnFamily.RELATION)) {
						OsmType = OSMType.RELATION;
					}
				}

				final ByteSequence CQ = item.getKey().getColumnQualifierData();
				if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.ID)) {
					Id = longReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.VERSION)) {
					Version = longReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.TIMESTAMP)) {
					Timestamp = longReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.CHANGESET)) {
					Changeset = longReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.USER_ID)) {
					UserId = longReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.USER_TEXT)) {
					UserName = stringReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.OSM_VISIBILITY)) {
					Visible = booleanReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.LATITUDE)) {
					Lattitude = doubleReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.LONGITUDE)) {
					Longitude = doubleReader.readField(
							item.getValue().get());
				}
				else if (Schema.arraysEqual(
						CQ,
						ColumnQualifier.REFERENCES)) {
					try {
						Nodes = TypeUtils.deserializeLongArray(
								item.getValue().get(),
								null).getIds();
					}
					catch (final IOException e) {
						log.error(
								"Error deserializing Avro encoded Relation member set",
								e);
					}
				}
				else if (Schema.startsWith(
						CQ,
						ColumnQualifier.REFERENCE_MEMID_PREFIX.getBytes(
								Schema.CHARSET))) {
					final String s = new String(
							CQ.toArray());
					final Integer id = Integer.valueOf(
							s.split(
									"_")[1]);
					if (relationSets == null) {
						relationSets = new HashMap<>();
					}
					if (!relationSets.containsKey(
							id)) {
						relationSets.put(
								id,
								new RelationSet());
					}
					relationSets.get(
							id).MemId = longReader.readField(
									item.getValue().get());
				}
				else if (Schema.startsWith(
						CQ,
						ColumnQualifier.REFERENCE_ROLEID_PREFIX.getBytes(
								Schema.CHARSET))) {
					final String s = new String(
							CQ.toArray());
					final Integer id = Integer.valueOf(
							s.split(
									"_")[1]);
					if (relationSets == null) {
						relationSets = new HashMap<>();
					}
					if (!relationSets.containsKey(
							id)) {
						relationSets.put(
								id,
								new RelationSet());
					}
					relationSets.get(
							id).RoleId = stringReader.readField(
									item.getValue().get());
				}
				else if (Schema.startsWith(
						CQ,
						ColumnQualifier.REFERENCE_TYPE_PREFIX.getBytes(
								Schema.CHARSET))) {
					final String s = new String(
							CQ.toArray());
					final Integer id = Integer.valueOf(
							s.split(
									"_")[1]);
					if (relationSets == null) {
						relationSets = new HashMap<>();
					}
					if (!relationSets.containsKey(
							id)) {
						relationSets.put(
								id,
								new RelationSet());
					}
					switch (stringReader.readField(
							item.getValue().get())) {
						case "NODE": {
							relationSets.get(
									id).MemType = MemberType.NODE;
							break;
						}
						case "WAY": {
							relationSets.get(
									id).MemType = MemberType.WAY;
							break;
						}
						case "RELATION": {
							relationSets.get(
									id).MemType = MemberType.RELATION;
							break;
						}
					}
				}
				else {
					// these should all be tags
					if (tags == null) {
						tags = new HashMap<>();
					}
					tags.put(
							item.getKey().getColumnQualifier().toString(),
							new String(
									item.getValue().get()));
				}
			}
		}
	}

	public static class RelationSet
	{
		public String RoleId = null;
		public Long MemId = null;
		public MemberType MemType = null;
	}

}
