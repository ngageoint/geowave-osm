package mil.nga.giat.osm.mapreduce.Convert;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.osm.mapreduce.Convert.OsmProvider.OsmProvider;
import mil.nga.giat.osm.mapreduce.Ingest.OSMMapperCommandArgs;

public class OSMConversionMapper extends
		Mapper<Key, Value, GeoWaveOutputKey, Object>
{

	private ByteArrayId indexId = null;
	private String globalVisibility = "";
	private final SimpleFeatureGenerator sfg = new SimpleFeatureGenerator();
	private OsmProvider osmProvider = null;

	@Override
	protected void map(
			final Key key,
			final Value value,
			final Context context )
			throws IOException,
			InterruptedException {
		final List<SimpleFeature> sf = sfg.mapOSMtoSimpleFeature(
				WholeRowIterator.decodeRow(
						key,
						value),
				osmProvider);
		if ((sf != null) && (sf.size() > 0)) {
			for (final SimpleFeature feat : sf) {
				final String name = feat.getType().getTypeName();
				context.write(
						new GeoWaveOutputKey(
								new ByteArrayId(name),
								indexId),
						feat);
			}
		}
	}

	@Override
	protected void cleanup(
			final Context context )
			throws IOException,
			InterruptedException {
		osmProvider.close();

		super.cleanup(
				context);
	}

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(
				context);
		try {
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY);
			final String primaryIndexIdStr = context.getConfiguration().get(
					AbstractMapReduceIngest.PRIMARY_INDEX_IDS_KEY);
			if (primaryIndexIdStr != null) {
				indexId = new ByteArrayId(
						primaryIndexIdStr);
			}
			final OSMMapperCommandArgs args = new OSMMapperCommandArgs();
			args.deserializeFromString(
					context.getConfiguration().get(
							"arguments"));
			osmProvider = new OsmProvider(
					args);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
