package mil.nga.giat.osm;

import mil.nga.giat.osm.mapreduce.OSMPBFRunner;
import org.apache.hadoop.fs.ContentSummary;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MRTests {


    @Before
    public void setUp() throws Exception {
        OSMTestEnvironment.Setup();
    }

    @After
    public void tearDown() throws Exception {
        OSMTestEnvironment.Shutdown();
    }

    @Test
    public void testStageData() throws Exception {
        OSMCommandArgs args = new OSMCommandArgs();
        args.nameNode = OSMTestEnvironment.getNameNode();
        args.ingestDirectory = OSMTestEnvironment.getLocalDataDirectory();
        OSMPBFStage stager = new OSMPBFStage();
        stager.StageData(args);
        ContentSummary cs = OSMTestEnvironment.getHDFSFileSummary(args.hdfsSequenceFile);
        Assert.assertEquals(cs.getLength(), 204006l);
    }

    @Test
    public void testPBFMapper() throws Exception {
        OSMPBFRunner runner = new OSMPBFRunner();
        runner.setConf(OSMTestEnvironment.getConf());
        //runner.run(new String[] {"-z", });
    }
}