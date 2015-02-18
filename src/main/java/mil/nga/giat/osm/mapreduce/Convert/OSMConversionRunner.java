package mil.nga.giat.osm.mapreduce.Convert;

import com.beust.jcommander.JCommander;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.AbstractMapReduceIngest;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.mapreduce.Ingest.OSMMapperCommandArgs;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinitionSet;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OSMConversionRunner
		extends Configured
		implements Tool
{
	private static final Logger LOGGER  = LoggerFactory.getLogger(OSMConversionRunner.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OSMConversionRunner(), args);
		System.exit(res);
	}


	@Override
	public int run(String[] args) throws Exception {

		OSMMapperCommandArgs argv = new OSMMapperCommandArgs();
		JCommander cmd = new JCommander(argv, args);
		Configuration conf = this.getConf();
		conf.set("osm_mapping", argv.getMappingContents());

		if (argv.visibility != null) {
			conf.set(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY,
					argv.visibility);
		}

		//job settings

		Job job = Job.getInstance(conf, argv.jobName + "NodeConversion");
		job.setJarByClass(OSMConversionRunner.class);

		//input format

		AccumuloInputFormat.setConnectorInfo(job, argv.user, new PasswordToken(argv.pass));
		AccumuloInputFormat.setInputTableName(job, argv.GetQualifiedTableName());
		AccumuloInputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(argv.instanceName).withZkHosts(argv.zookeepers));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations(argv.visibility));

		IteratorSetting is = new IteratorSetting(50, "WholeRow", WholeRowIterator.class);
		AccumuloInputFormat.addIterator(job,is);
		job.setInputFormatClass(AccumuloInputFormat.class);
		Range r = new Range();
		ArrayList<Pair<Text, Text>> columns = new ArrayList<>();
		columns.add(new Pair<>(new Text(Schema.CF.NODE), new Text()));

		AccumuloInputFormat.setRanges(job, Arrays.asList(r));
		AccumuloInputFormat.fetchColumns(job, new ArrayList<Pair<Text, Text>>());

		//output format
		GeoWaveOutputFormat.setAccumuloOperationsInfo(job, argv.zookeepers, argv.instanceName, argv.user, argv.pass, argv.osmNamespace);
		for (FeatureDataAdapter fda : FeatureDefinitionSet.featureAdapters.values()){
			GeoWaveOutputFormat.addDataAdapter(job, fda);
		}

		Index primaryIndex = IndexType.SPATIAL_RASTER.createDefaultIndex();
		GeoWaveOutputFormat.addIndex(job, primaryIndex);
		conf.set(AbstractMapReduceIngest.PRIMARY_INDEX_ID_KEY,
				StringUtils.stringFromBinary(primaryIndex.getId().getBytes()));

		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setMapOutputKeyClass(GeoWaveOutputKey.class);
		job.setMapOutputValueClass(SimpleFeature.class);

		//mappper

		job.setMapperClass(OSMConversionMapper.class);

		//reducer
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
