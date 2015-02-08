package mil.nga.giat.osm;

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CommandArgsTest {


    @Test
    public void TestAccumuloArgumentParsing() {

        String z = "zookeeper1:port,zookeeper2:port";
        String i = "accumulo_instance";
        String au = "accumulo_username";
        String ap = "accumulo_password";
        String n = "osm_namespace";
        String v = "osm_default_visibility";
        String in = "input_directory";
        String out = "hdfs_output_sequence_file";
        String nn = "hdfs_namenode:port";
        Boolean dropTable = true;


		String[] argv = buildArgsString(z, i, au, ap, n, v, in, out, nn, dropTable);
        final OSMCommandArgs osmArgs = new OSMCommandArgs();
        final JCommander cmd = new JCommander(osmArgs, argv);

        Assert.assertEquals(dropTable, osmArgs.dropOSMData);
        Assert.assertEquals(z, osmArgs.zookeepers);
        Assert.assertEquals(i, osmArgs.instanceName);
        Assert.assertEquals(au, osmArgs.user);
        Assert.assertEquals(ap, osmArgs.pass);
        Assert.assertEquals(n, osmArgs.osmNamespace);
        Assert.assertEquals(v, osmArgs.visibility);
        Assert.assertEquals(in, osmArgs.ingestDirectory);
        Assert.assertEquals(out, osmArgs.hdfsBasePath);
        Assert.assertEquals(nn, osmArgs.nameNode);
    }


	protected static String[] buildArgsString(String zookeepers, String accumuloInstance, String accumuloUsername,
			String accumuloPassword, String osmNamespace, String osmVisibility, String inputDirectory, String hdfsBasePath, String namenode, boolean dropTable){

		return new String[] {"-z", zookeepers, "-i", accumuloInstance, "-au", accumuloUsername, "-ap", accumuloPassword, "-n", osmNamespace, "-v", osmVisibility, "-in", inputDirectory, "-out", hdfsBasePath, "-nn", namenode, dropTable ? "--dropOSMData" : ""};
	}


}