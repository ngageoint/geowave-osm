package mil.nga.giat.osm.mapreduce;

import com.beust.jcommander.JCommander;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OSMPBFRunner extends Configured implements Tool {
    private static final Logger log = LoggerFactory.getLogger(OSMPBFRunner.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OSMPBFRunner(), args);
        System.exit(res);
    }

    private void enableLocalityGroups(OSMPBFMapperCommandArgs argv) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        BasicAccumuloOperations bao = new BasicAccumuloOperations(argv.zookeepers,argv.instanceName,argv.user,argv.pass,argv.osmNamespace);
        bao.createTable(argv.osmTableName);

        bao.addLocalityGroup(argv.osmTableName, Schema.CF.NODE);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.NODE_TAG);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.WAY);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.WAY_TAG);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.RELATION);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.RELATION_TAG);
    }

    @Override
    public int run(String[] args) throws Exception {

        OSMPBFMapperCommandArgs argv = new OSMPBFMapperCommandArgs();
        JCommander cmd = new JCommander(argv, args);
        Configuration conf = this.getConf();
        conf.set("tableName", argv.GetQualifiedTableName());
        conf.set("osmVisibility", argv.visibility);

        enableLocalityGroups(argv);

        //job settings
        Job job = Job.getInstance(conf, argv.jobName);
        job.setJarByClass(OSMPBFRunner.class);

        //input format
        SequenceFileInputFormat.setInputPaths(job, argv.hdfsBasePath);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        //mappper
        job.setMapperClass(OSMPBFMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setConnectorInfo(job, argv.user, new PasswordToken(argv.pass));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, argv.GetQualifiedTableName());
        AccumuloOutputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(argv.instanceName).withZkHosts(argv.zookeepers));

        //reducer
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : -1;
    }

}
