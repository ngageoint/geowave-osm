package mil.nga.giat.osm.mapreduce;

import com.beust.jcommander.Parameter;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;


public class OSMPBFMapperCommandArgs {

    protected OSMPBFMapperCommandArgs(){}

    protected OSMPBFMapperCommandArgs(String zookeepers, String instanceName, String user, String pass, String osmNamespace, String visibility, String hdfsSequenceFile, String jobName){
        this.zookeepers = zookeepers;
        this.instanceName = instanceName;
        this.user = user;
        this.pass = pass;
        this.osmNamespace = osmNamespace;
        this.visibility = visibility;
        this.hdfsSequenceFile = hdfsSequenceFile;
        this.jobName = jobName;
    }

    @Parameter(names = {"-z","--zookeepers"} , required = false, description = "list of zookeeper:port instances, comma separated")
    protected String zookeepers;

    @Parameter(names = {"-i","--instanceName"}, required = false, description = "accumulo instance name")
    protected String instanceName;

    @Parameter(names = {"-au","--accumuloUser"}, required = false, description = "accumulo username")
    protected String user;

    @Parameter(names = {"-ap","--accumuloPass"}, required = false, description = "accumulo password")
    protected String pass;

    @Parameter(names = {"-n","--osmNamespace"}, required = false, description = "namespace for OSM data")
    protected String osmNamespace;

    @Parameter(names = {"-v","--osmDefaultVisibility"}, required = false, description = "default visibility for  OSM data.")
    protected String visibility = "public";

    @Parameter(names = {"-out", "--hdfsSquenceFile"}, required = false, description = "file to stage hdfs files to  - user must have write permissions")
    protected String hdfsSequenceFile = "/user/" + System.getProperty("user.name") + "/osm_stage";

    @Parameter(names = {"-jn", "--jobName"}, required = false, description = "Name of mapreduce job")
    protected String jobName = "PBF Ingest (" + System.getProperty("user.name") + ")";

    protected String osmTableName = "OSM";

    protected String GetQualifiedTableName(){
        return AccumuloUtils.getQualifiedTableName(osmNamespace,osmTableName);
    }

}
