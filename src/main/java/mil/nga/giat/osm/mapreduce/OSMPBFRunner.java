package mil.nga.giat.osm.mapreduce;

import com.beust.jcommander.JCommander;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.types.generated.Node;
import mil.nga.giat.osm.types.generated.Relation;
import mil.nga.giat.osm.types.generated.Way;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OSMPBFRunner extends Configured implements Tool {
    private static final Logger log = LoggerFactory.getLogger(OSMPBFRunner.class);
    private org.apache.avro.Schema avroSchema = null;
    private String inputAvroFile = null;

    public static void main(String[] args) throws Exception {
        OSMMapperCommandArgs argv = new OSMMapperCommandArgs();
        JCommander cmd = new JCommander(argv, args);
        OSMPBFRunner runner = new OSMPBFRunner();

        int res = ToolRunner.run(new Configuration(), runner, args);
        System.exit(res);
    }

    public void configureSchema(org.apache.avro.Schema avroSchema){
        this.avroSchema = avroSchema;
    }

    private void enableLocalityGroups(OSMMapperCommandArgs argv) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        BasicAccumuloOperations bao = new BasicAccumuloOperations(argv.zookeepers,argv.instanceName,argv.user,argv.pass,argv.osmNamespace);
        bao.createTable(argv.osmTableName);

        bao.addLocalityGroup(argv.osmTableName, Schema.CF.NODE);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.WAY);
        bao.addLocalityGroup(argv.osmTableName, Schema.CF.RELATION);
    }

    @Override
    public int run(String[] args) throws Exception {

        OSMMapperCommandArgs argv = new OSMMapperCommandArgs();
        JCommander cmd = new JCommander(argv, args);
        Configuration conf = this.getConf();
        conf.set("tableName", argv.GetQualifiedTableName());
        conf.set("osmVisibility", argv.visibility);

        //job settings
        Job job = Job.getInstance(conf, argv.jobName);
        job.setJarByClass(OSMPBFRunner.class);

        switch (argv.mapperType){
            case "NODE": {
                configureSchema(Node.getClassSchema());
                inputAvroFile = argv.getNodesBasePath();
                job.setMapperClass(OSMNodeMapper.class);
                break;
            }
            case "WAY": {
                configureSchema(Way.getClassSchema());
                inputAvroFile = argv.getWaysBasePath();
                job.setMapperClass(OSMWayMapper.class);
                break;
            }
            case "RELATION": {
                configureSchema(Relation.getClassSchema());
                inputAvroFile = argv.getRelationsBasePath();
                job.setMapperClass(OSMRelationMapper.class);
                break;
            }
        }
        if (avroSchema == null || inputAvroFile == null){
            throw new MissingArgumentException("argument for mapper type must be one of: NODE, WAY, or RELATION");
        }




        enableLocalityGroups(argv);

        //input format
		job.setInputFormatClass(AvroKeyInputFormat.class);
		FileInputFormat.setInputPaths(job, inputAvroFile);
		AvroJob.setInputKeySchema(job, avroSchema);

        //mappper

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
