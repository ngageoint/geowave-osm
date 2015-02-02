package mil.nga.giat.osm;

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Test;

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

        String[] argv = {"-z", z, "-i", i, "-au", au, "-ap", ap, "-n", n, "-v", v, "-in", in, "-out", out, "-nn", nn, dropTable ? "--dropOSMData" : ""};
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
        Assert.assertEquals(out, osmArgs.hdfsSequenceFile);
        Assert.assertEquals(nn, osmArgs.nameNode);
    }
}