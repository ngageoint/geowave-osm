package mil.nga.giat.osm.mapreduce.Convert;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithMapper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.Map;

public class OSMConversionMapper extends Mapper<Key, Value, GeoWaveOutputKey, Object>{

	private static ByteArrayId indexId = null;
	private static String globalVisibility = "";

	@Override protected void map( Key key, Value value, Context context )
			throws IOException, InterruptedException {
		ByteArrayId adapterId = null;
		SimpleFeature sf = null;

		for (Map.Entry<Key,Value> entry2 : WholeRowIterator.decodeRow(key, value).entrySet()){


		}
		context.write(new GeoWaveOutputKey(adapterId, indexId), sf);
	}

	@Override protected void setup( Context context )
			throws IOException, InterruptedException {
		super.setup(context);
		try {
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY);
			final String primaryIndexIdStr = context.getConfiguration().get(
					AbstractMapReduceIngest.PRIMARY_INDEX_ID_KEY);
			if (primaryIndexIdStr != null) {
				indexId = new ByteArrayId(
						primaryIndexIdStr);
			}
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
