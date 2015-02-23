package mil.nga.giat.osm.osmfeature;

import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinitionSet;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class FeatureConfigParserTest
{

	protected static final String TEST_RESOURCE_DIR = new File("./src/test/data/").getAbsolutePath().toString();
	protected static final String TEST_DATA_CONFIG = TEST_RESOURCE_DIR + "/" + "test_mapping.json";


	@Test
	public void testFeatureConfigParser()
			throws IOException {
		FeatureConfigParser fcp = new FeatureConfigParser();

		FeatureDefinitionSet fds = null;

		FileInputStream fis = new FileInputStream(new File(TEST_DATA_CONFIG));


		//fds = fcp.parseConfig(fis);


		fis.close();




	}

}