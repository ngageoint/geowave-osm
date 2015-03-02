package mil.nga.giat.osm.mapreduce.Convert.OsmProvider;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.*;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.data.field.BasicReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.mapreduce.Convert.SimpleFeatureGenerator;
import mil.nga.giat.osm.mapreduce.Ingest.OSMMapperCommandArgs;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinition;
import mil.nga.giat.osm.types.TypeUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class OsmProvider
{

	private static final Logger LOGGER = LoggerFactory.getLogger(OsmProvider.class);
	private Connector conn = null;
	private OSMMapperCommandArgs args;
	private BatchScanner bs = null;
	private final BasicWriter.LongWriter longWriter = new BasicWriter.LongWriter();
	private final BasicReader.LongReader longReader = new BasicReader.LongReader();
	private final BasicReader.DoubleReader doubleReader = new BasicReader.DoubleReader();
	private static final byte EMPTY_BYTES[] = new byte[0];

	public OsmProvider(OSMMapperCommandArgs args)
			throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
		this.args = args;
		conn = new ZooKeeperInstance(args.instanceName, args.zookeepers).getConnector(args.user, new PasswordToken(args.pass));
		bs = conn.createBatchScanner(args.getQualifiedTableName(), new Authorizations(args.visibility), 1);
	}

	public Geometry processRelation(SimpleFeatureGenerator.OSMUnion osmunion, FeatureDefinition fd){

		//multipolygon type
		if (osmunion.relationSets != null && osmunion.relationSets.size() > 0 && osmunion.tags != null && "multipolygon".equals(osmunion.tags.get("type"))){

			Map<String, List<LinearRing>> rings = waysFromAccumulo(osmunion.relationSets, osmunion);

			if (rings == null){
				return null;
			}


			List<LinearRing> outer = rings.get("outer");
			List<LinearRing> inner = rings.get("inner");

			if (outer.size() == 0){
				LOGGER.error("Polygons must have at least one outer ring; error with relation: " + osmunion.Id);
				return null;
			}

			List<Polygon> polygons = new ArrayList<Polygon>();

			for (LinearRing lr : outer){
				List<LinearRing> tempInner = new ArrayList<>();
				for (LinearRing i : inner){
					if (lr.contains(i)){
						tempInner.add(i);
					}
				}
				polygons.add(GeometryUtils.GEOMETRY_FACTORY.createPolygon(lr, tempInner.toArray(new LinearRing[tempInner.size()])));
			}

			if (polygons.size() == 0){
				LOGGER.error("No polygons built for relation: " + osmunion.Id);
				return null;
			}

			if (polygons.size() == 1){
				return polygons.get(0);
			}

			return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));

		}
		LOGGER.info("Unsupported relation type for relation: " + osmunion.Id);
		//todo admin boundaries, routes, etc: http://wiki.openstreetmap.org/wiki/Types_of_relation
		return null;
	}

	public Geometry processWay(SimpleFeatureGenerator.OSMUnion osmunion, FeatureDefinition fd)	{

		if (osmunion.Nodes == null || osmunion.Nodes.size() == 0) {
			return null;
		}


		Map<Long, Coordinate> coords =  nodesFromAccumulo(osmunion.Nodes);
		Coordinate[] orderedCoords = new Coordinate[osmunion.Nodes.size()];


		List<String> missingNodes = new ArrayList<>();

		int i = 0;
		for (long l : osmunion.Nodes){
			//String hash = new String(Schema.getIdHash(l));

			orderedCoords[i] = (coords.get(l));
			if (orderedCoords[i] == null){
				//System.out.println("missing point for way: " + osmunion.Id);
				missingNodes.add(String.valueOf(l));
			}
			i++;
		}

		//if we are missing portions geometry is invalid;  log it and return null
		if (missingNodes.size() != 0) {
			LOGGER.error("[2]Some of the nodes for Way: " + osmunion.Id + " were not present.  Nodes missing were: (" + Joiner.on(",").join(missingNodes) + ")");
			return null;
		} else {
			LOGGER.info("[2]Successfully built way: " + osmunion.Id);
		}

		if (osmunion.Nodes.size() > 2 && osmunion.Nodes.get(0) == osmunion.Nodes.get(osmunion.Nodes.size() - 1)){
			//closed way
			switch (fd.Type){
				case Geometry: { //best guess on type = polygon (closed way)
					return GeometryUtils.GEOMETRY_FACTORY.createPolygon(orderedCoords);
				}
				case Polygon: {
					return GeometryUtils.GEOMETRY_FACTORY.createPolygon(orderedCoords);
				}
				case LineString: {
					return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords);
				}
				case Point: {
					return GeometryUtils.GEOMETRY_FACTORY.createPolygon(orderedCoords).getCentroid();
				}
			}
		} else {
			//open way
			switch (fd.Type){
				case Geometry: { //best guess on type
					String area = osmunion.tags.get("area");
					if (area != null && "yes".equals(area)){
						//close the geometry - it's supposto be an area
						Coordinate[] closedCords  = Arrays.copyOf(orderedCoords, orderedCoords.length + 1);
						closedCords[closedCords.length - 1] = closedCords[0];
						return GeometryUtils.GEOMETRY_FACTORY.createPolygon(closedCords);
					}
					else return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords);

				}
				case Polygon: {
					if (orderedCoords.length < 3){
						LOGGER.warn("Geometry type Polygon requested for unclosed way, but not enough points (4) would be present after closing.  Relation id: " + osmunion.Id);
						return null;
					}
					//close the geometry since it's unclosed, but coereced to a polygon
					Coordinate[] closedCords  = Arrays.copyOf(orderedCoords, orderedCoords.length + 1);
					closedCords[closedCords.length - 1] = closedCords[0];
					return GeometryUtils.GEOMETRY_FACTORY.createPolygon(closedCords);
				}
				case LineString: {
					return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords);
				}
				case Point: {
					return GeometryUtils.GEOMETRY_FACTORY.createLineString(orderedCoords).getCentroid();
				}
			}
		}

		//default case, shouldn't be hit;
		LOGGER.error("Way: " + osmunion.Id + " did not parse correctly; geometry generation was not caught and fell through");
		return null;
	}


	public void close(){
		if (bs != null){
			bs.close();
		}
	}

	private Map<String,List<LinearRing>> waysFromAccumulo(Map<Integer, SimpleFeatureGenerator.RelationSet> relations, SimpleFeatureGenerator.OSMUnion osmunion){

		Map<String, List<LinearRing>> rings = new HashMap<>();
		rings.put("inner", new ArrayList<LinearRing>());
		rings.put("outer", new ArrayList<LinearRing>());


		List<Long> outerWays = new ArrayList<>();
		List<Long> innerWays = new ArrayList<>();

		for (Map.Entry<Integer, SimpleFeatureGenerator.RelationSet> kvp : relations.entrySet()) {
			switch (kvp.getValue().MemType){
				case RELATION: {
					LOGGER.warn("Super-relations not currently supported");
					return null;
				}
				case WAY: {
					if ("outer".equals(kvp.getValue().RoleId)){
						outerWays.add(kvp.getValue().MemId);
					}
					else if ("inner".equals(kvp.getValue().RoleId)){
						innerWays.add(kvp.getValue().MemId);
					}
					break;
				}
				case NODE: {
					LOGGER.warn("Nodes as direct members of relationships not currently supported");
					return null;
				}
			}

		}

		List<Range> ranges = new ArrayList<>(outerWays.size() + innerWays.size());
		if (ranges.size() == 0){
			LOGGER.warn("No multipolygon relations found for relation: " + osmunion.Id);
			return null;
		}

		for (Long l : outerWays){
			byte[] row = Schema.getIdHash(l);
			ranges.add(new Range(new Text(row)));
		}
		for (Long l : innerWays) {
			byte[] row = Schema.getIdHash(l);
			ranges.add(new Range(new Text(row)));
		}

		bs.setRanges(ranges);
		bs.clearColumns();
		bs.fetchColumn(new Text(Schema.CF.WAY), new Text(Schema.CQ.ID));
		bs.fetchColumn(new Text(Schema.CF.WAY), new Text(Schema.CQ.REFERENCES));

		Map<Long, List<Long>> vals = new HashMap<>();

		long id = -1;
		List<Long> tvals = null;
		ByteSequence lastkey = null;

		for (Map.Entry<Key, Value> row: bs){
			if (lastkey == null){
				lastkey = row.getKey().getRowData();
			}

			if (Schema.arraysEqual(row.getKey().getColumnQualifierData(), Schema.CQ.ID)){
				id = longReader.readField(row.getValue().get());
			}
			else if (Schema.arraysEqual(row.getKey().getColumnQualifierData(), Schema.CQ.REFERENCES)){
				try {
					tvals = TypeUtils.deserializeLongArray(row.getValue().get(), null).getIds();
				}
				catch (IOException e) {
					LOGGER.error("Error deserializing member array for way: ");
				}
			}

			if (id != -1 && tvals != null){
				vals.put(id, tvals);
				tvals = null;
				id = -1;
				lastkey = null;
			} else if (!lastkey.equals(row.getKey().getRowData())){
				tvals = null;
				id = -1;
				lastkey = null;
			}

		}

		for (Map.Entry<Long, List<Long>> kvp : vals.entrySet()){
			Map<Long, Coordinate> ring = nodesFromAccumulo(kvp.getValue());
			Coordinate[] sortedCoords = new Coordinate[ring.size()];
			List<String> missingIds = new ArrayList<>();
			int i = 0;
			for (long l : kvp.getValue()){
				sortedCoords[i] = ring.get(l);
				if (sortedCoords[i] == null){

					missingIds.add(String.valueOf(l));
				}
				i++;
			}
			if (missingIds.size() != 0){
				LOGGER.error("Error building ring relation for relation: " + osmunion.Id + " missing values were: (" + Joiner.on(",").join(missingIds) + ")");
				return null;
			}



			if (sortedCoords[0] != sortedCoords[sortedCoords.length - 1]){
				//ring not closed, should be by definition -f ix
				Coordinate[] closedCords = Arrays.copyOf(sortedCoords, sortedCoords.length + 1);
				closedCords[sortedCoords.length + 1] = closedCords[0];
				sortedCoords = closedCords;
			}

			if (sortedCoords.length < 4){
				LOGGER.error("Not enough coordinates for way: " + kvp.getKey() + " for relation: " + osmunion.Id);
				return null;
			}

			LinearRing lr = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(sortedCoords);

			if (innerWays.contains(kvp.getKey())){
				rings.get("inner").add(lr);
			} else if (outerWays.contains(kvp.getKey())){
				rings.get("outer").add(lr);
			} else {
				LOGGER.error("Relation not found in inner or outer for way: " + kvp.getKey());
				return null;
			}

		}
		return rings;
	}


	private Map<Long, Coordinate> nodesFromAccumulo(List<Long> vals){

		List<Range> ranges = new ArrayList<>(vals.size());
		for (Long l : vals){
			byte[] row = Schema.getIdHash(l);
			ranges.add(new Range(new Text(row)));
			//ranges.add(new Range(l.toString()));
		}
		ranges = Range.mergeOverlapping(ranges);

		bs.setRanges(ranges);
		bs.clearColumns();
		//bs.fetchColumnFamily(new Text(Schema.CF.NODE));
		bs.fetchColumn(new Text(Schema.CF.NODE), new Text(Schema.CQ.LONGITUDE));
		bs.fetchColumn(new Text(Schema.CF.NODE), new Text(Schema.CQ.LATITUDE));
		bs.fetchColumn(new Text(Schema.CF.NODE), new Text(Schema.CQ.ID));

		Map<Long, Coordinate> coords = new HashMap<>();

		long id = -1;
		Coordinate crd = new Coordinate(-1, -1);
		ByteSequence lastkey = null;

		for (Map.Entry<Key, Value> row : bs){
			if (lastkey == null){
				lastkey = row.getKey().getRowData();
			}

			if (Schema.arraysEqual(row.getKey().getColumnQualifierData(), Schema.CQ.LONGITUDE)){
				crd.x = doubleReader.readField(row.getValue().get());
			} else if (Schema.arraysEqual(row.getKey().getColumnQualifierData(), Schema.CQ.LATITUDE)) {
				crd.y = doubleReader.readField(row.getValue().get());
			} else if (Schema.arraysEqual(row.getKey().getColumnQualifierData(), Schema.CQ.ID)){
				id = longReader.readField(row.getValue().get());
			}

			if (id != -1 && crd.x != -1 && crd.y != -1){
				coords.put(id, crd);
				id = -1;
				crd = new Coordinate(-1, -1);
				lastkey = null;
			} else if (!lastkey.equals(row.getKey().getRowData())){
				id = -1;
				crd = new Coordinate(-1, -1);
				lastkey = null;
			}

		}
		return coords;
	}
}

