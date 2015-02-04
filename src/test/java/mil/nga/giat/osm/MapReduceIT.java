package mil.nga.giat.osm;

import mil.nga.giat.geowave.test.GeoWaveDFSTestEnvironment;
import mil.nga.giat.geowave.test.MapReduceTestEnvironment;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MapReduceIT
		extends GeoWaveDFSTestEnvironment
{

	private final static Logger LOGGER = LoggerFactory.getLogger(MapReduceIT.class);


	protected static final String TEST_RESOURCE_DIR = new File("./src/test/data/").getAbsolutePath().toString();
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_DIR + "/" + "hangzhou_china.zip";
	protected static final String TEST_DATA_BASE_DIR = new File("./target/data/").getAbsoluteFile().toString();



	@BeforeClass
	public static void setupTestData()
			throws ZipException {
			ZipFile data = new ZipFile(new File(TEST_DATA_ZIP_RESOURCE_PATH));
			data.extractAll(TEST_DATA_BASE_DIR);

	}


	@Test
	public void testIngestOSMPBF()
			throws IOException {
		OSMCommandArgs args = new OSMCommandArgs();
		args.nameNode = NAME_NODE;
		args.ingestDirectory = TEST_DATA_BASE_DIR;
		OSMPBFStage stager = new OSMPBFStage();
		stager.StageData(args);
		ContentSummary cs = getHDFSFileSummary(args.hdfsSequenceFile);
		Assert.assertEquals(cs.getLength(), 204006l);
		DEFER_CLEANUP = true;  //todo - figure out what's conflicting with accumulo cleanup
	}


	private static ContentSummary getHDFSFileSummary(String filename) throws IOException {
				org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filename);
				FileSystem file = path.getFileSystem(CONF);
				ContentSummary cs =  file.getContentSummary(path);
				file.close();
				return cs;
	}


}
