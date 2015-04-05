package mil.nga.giat.osm.mapreduce.Convert;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.data.field.BasicReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.osm.mapreduce.Convert.OsmProvider.OsmProvider;
import mil.nga.giat.osm.mapreduce.Ingest.OSMMapperCommandArgs;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OSMConversionMapper extends Mapper<Key, Value, GeoWaveOutputKey, Object>{

	private ByteArrayId indexId = null;
    private AdapterStore as = null;
	private String globalVisibility = "";
	private final SimpleFeatureGenerator sfg = new SimpleFeatureGenerator();
	private OsmProvider osmProvider = null;
    private final Map<String, ByteArrayId> featureAdapters = new HashMap<>();

    //ToDo: remove -just for testing
    private static final Map<String, Long> featureCounts = new HashMap<>();



	@Override protected void map( Key key, Value value, Context context )
			throws IOException, InterruptedException {
		ByteArrayId adapterId = null;

		List<SimpleFeature> sf = sfg.mapOSMtoSimpleFeature(WholeRowIterator.decodeRow(key, value), osmProvider);
		if (sf != null && sf.size() > 0){
			for (SimpleFeature feat : sf){
                String name = feat.getType().getTypeName();
                if (!featureAdapters.containsKey(name)){
                    FeatureDataAdapter fda = new FeatureDataAdapter(feat.getType());
                    featureAdapters.put(name, fda.getAdapterId());
                    featureCounts.put(name, 0l);
                }
                context.write(new GeoWaveOutputKey(featureAdapters.get(name), indexId), feat);
                featureCounts.put(name, featureCounts.get(name) + 1l);
			}
		}
	}


	@Override
	protected void cleanup(Context context
	) throws IOException, InterruptedException {
		osmProvider.close();

        //ToDo: remove, testing only
        System.out.println("******************************************************");
        for (Map.Entry<String, Long> kvp : featureCounts.entrySet()){
            System.out.println("Feature: " + kvp.getKey() + " [" + kvp.getValue() + "]");
        }
        System.out.println("******************************************************");

		super.cleanup(context);
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
			OSMMapperCommandArgs args = new OSMMapperCommandArgs();
			args.deserializeFromString(context.getConfiguration().get("arguments"));
			osmProvider = new OsmProvider(args);
            as = new AccumuloAdapterStore(GeoWaveOutputFormat.getAccumuloOperations(context));
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
