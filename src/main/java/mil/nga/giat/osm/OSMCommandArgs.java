package mil.nga.giat.osm;

import com.beust.jcommander.Parameter;

/**
 * Created by bennight on 2/1/2015.
 */
public class OSMCommandArgs {

    @Parameter(names = {"-z","--zookeepers"} , required = false, description = "list of zookeeper:port instances, comma separated")
    protected String zookeepers;

    @Parameter(names = {"-i","--instanceName"}, required = false, description = "accumulo instance name")
    protected String instanceName;

    @Parameter(names = {"-au","--accumuloUser"}, required = false, description = "accumulo username")
    protected String user;

    @Parameter(names = {"-ap","--accumuloPass"}, required = false, description = "accumulo password", password = true)
    protected String pass;

    @Parameter(names = {"-n","--osmNamespace"}, required = false, description = "namespace for OSM data")
    protected String osmNamespace;

    @Parameter(names = {"-v","--osmDefaultVisibility"}, required = false, description = "default visibility for  OSM data.")
    protected String visibility = "public";

    @Parameter(names = {"-d","--dropOSMData"}, required = false, description = "delete all OSM data for the specified namespace")
    protected boolean dropOSMData;

    @Parameter(names = {"-in", "--inputDirectory"}, required = false, description = "directory to ingest files from - will match all files with the .pbf extension")
    protected String ingestDirectory;

    @Parameter(names = {"-out", "--hdfsOutput"}, required = false, description = "directory to stage hdfs files to  - user must have write permissions")
    protected String hdfsDirectory = "/user/" + System.getProperty("user.name");

    @Parameter(names = {"-nn", "--hdfsNamenode"}, required = false, description = "hdfs namenode in the format hostname:port")
    protected String nameNode;

    protected String extension = ".pbf";

}
