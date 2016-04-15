package mil.nga.giat.osm.mapreduce.Convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opengis.feature.simple.SimpleFeature;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.osm.mapreduce.Ingest.OSMMapperCommandArgs;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinitionSet;

public class OSMConversionRunner extends
		Configured implements
		Tool
{

	public static void main(
			final String[] args )
			throws Exception {
		final int res = ToolRunner.run(
				new Configuration(),
				new OSMConversionRunner(),
				args);
		System.exit(
				res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {

		final OSMMapperCommandArgs argv = new OSMMapperCommandArgs();
		new JCommander(
				argv,
				args);
		final Configuration conf = getConf();

		// job settings

		final Job job = Job.getInstance(
				conf,
				argv.jobName + "NodeConversion");
		job.setJarByClass(
				OSMConversionRunner.class);

		job.getConfiguration().set(
				"osm_mapping",
				argv.getMappingContents());
		job.getConfiguration().set(
				"arguments",
				argv.serializeToString());

		if (argv.visibility != null) {
			job.getConfiguration().set(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY,
					argv.visibility);
		}

		// input format

		AbstractInputFormat.setConnectorInfo(
				job,
				argv.user,
				new PasswordToken(
						argv.pass));
		InputFormatBase.setInputTableName(
				job,
				argv.getQualifiedTableName());
		AbstractInputFormat.setZooKeeperInstance(
				job,
				new ClientConfiguration().withInstance(
						argv.instanceName).withZkHosts(
								argv.zookeepers));
		AbstractInputFormat.setScanAuthorizations(
				job,
				new Authorizations(
						argv.visibility));

		final IteratorSetting is = new IteratorSetting(
				50,
				"WholeRow",
				WholeRowIterator.class);
		InputFormatBase.addIterator(
				job,
				is);
		job.setInputFormatClass(
				AccumuloInputFormat.class);
		final Range r = new Range();
		final ArrayList<Pair<Text, Text>> columns = new ArrayList<>();
		InputFormatBase.setRanges(
				job,
				Arrays.asList(
						r));

		// output format
		GeoWaveOutputFormat.setDataStoreName(
				job.getConfiguration(),
				"accumulo");
		final Map<String, String> storeConfigOptions = new HashMap<String, String>();
		storeConfigOptions.put(
				BasicAccumuloOperations.ZOOKEEPER_CONFIG_NAME,
				argv.zookeepers);
		storeConfigOptions.put(
				BasicAccumuloOperations.INSTANCE_CONFIG_NAME,
				argv.instanceName);
		storeConfigOptions.put(
				BasicAccumuloOperations.USER_CONFIG_NAME,
				argv.user);
		storeConfigOptions.put(
				BasicAccumuloOperations.PASSWORD_CONFIG_NAME,
				argv.pass);
		GeoWaveOutputFormat.setStoreConfigOptions(
				job.getConfiguration(),
				storeConfigOptions);
		GeoWaveOutputFormat.setGeoWaveNamespace(
				job.getConfiguration(),
				"");

		final AdapterStore as = new AccumuloAdapterStore(
				new BasicAccumuloOperations(
						argv.zookeepers,
						argv.instanceName,
						argv.user,
						argv.pass,
						argv.osmNamespace));
		for (final FeatureDataAdapter fda : FeatureDefinitionSet.featureAdapters.values()) {
			as.addAdapter(
					fda);
			GeoWaveOutputFormat.addDataAdapter(
					job.getConfiguration(),
					fda);
		}

		final PrimaryIndex primaryIndex = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				primaryIndex);
		job.getConfiguration().set(
				AbstractMapReduceIngest.PRIMARY_INDEX_IDS_KEY,
				StringUtils.stringFromBinary(
						primaryIndex.getId().getBytes()));

		job.setOutputFormatClass(
				GeoWaveOutputFormat.class);
		job.setMapOutputKeyClass(
				GeoWaveOutputKey.class);
		job.setMapOutputValueClass(
				SimpleFeature.class);

		// mappper

		job.setMapperClass(
				OSMConversionMapper.class);

		// reducer
		job.setNumReduceTasks(
				0);

		return job.waitForCompletion(
				true) ? 0 : -1;
	}
}
