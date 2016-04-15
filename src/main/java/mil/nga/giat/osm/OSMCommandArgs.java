package mil.nga.giat.osm;

import com.beust.jcommander.Parameter;

public class OSMCommandArgs {

    protected OSMCommandArgs(){}

    protected OSMCommandArgs(String zookeepers, String instanceName, String user, String pass, String osmNamespace, String visibility, Boolean dropOSMData, String ingestDirectory, String hdfsBasePath, String nameNode){
        this.zookeepers = zookeepers;
        this.instanceName = instanceName;
        this.user = user;
        this.pass = pass;
        this.osmNamespace = osmNamespace;
        this.visibility = visibility;
        this.dropOSMData = dropOSMData;
        this.ingestDirectory = ingestDirectory;
        this.hdfsBasePath = hdfsBasePath;
        this.nameNode = nameNode;
    }

    @Parameter(names = {"-z","--zookeepers"} , required = false, description = "list of zookeeper:port instances, comma separated")
	public String zookeepers;

    @Parameter(names = {"-i","--instanceName"}, required = false, description = "accumulo instance name")
	public String instanceName;

    @Parameter(names = {"-au","--accumuloUser"}, required = false, description = "accumulo username")
	public String user;

    @Parameter(names = {"-ap","--accumuloPass"}, required = false, description = "accumulo password")
	public String pass;

    @Parameter(names = {"-n","--osmNamespace"}, required = false, description = "namespace for OSM data")
	public String osmNamespace;

    @Parameter(names = {"-v","--osmDefaultVisibility"}, required = false, description = "default visibility for  OSM data.")
	public String visibility = "public";

    @Parameter(names = {"--dropOSMData"}, required = false, description = "delete all OSM data for the specified namespace")
	public boolean dropOSMData;

    @Parameter(names = {"-in", "--inputDirectory"}, required = false, description = "directory to ingest files from - will match all files with the .pbf extension")
	public String ingestDirectory;

    @Parameter(names = {"-out", "--hdfsBasePath"}, required = false, description = "directory to stage hdfs files to  - user must have write permissions")
	public String hdfsBasePath = "/user/" + System.getProperty("user.name") + "/osm_stage/";

    @Parameter(names = {"-nn", "--hdfsNamenode"}, required = false, description = "hdfs namenode in the format hostname:port")
	public String nameNode;

	public String extension = ".pbf";


	public String getNodesBasePath(){
		return hdfsBasePath + "/nodes";
	}

	public String getWaysBasePath(){
		return hdfsBasePath + "/ways";
	}

	public String getRelationsBasePath(){
		return hdfsBasePath + "/relations";
	}



}
