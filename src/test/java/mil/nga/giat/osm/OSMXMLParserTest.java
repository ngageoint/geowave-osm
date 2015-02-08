package mil.nga.giat.osm;


import mil.nga.giat.osm.parser.OsmXmlLoader;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class OSMXMLParserTest
{

	@Test
	public void testParseToCollection()
			throws Exception {

		String file = "D:\\Projects\\geowave-osm\\target\\data\\hangzhou_china.osm";

		OsmXmlLoader x = OsmXmlLoader.readOsmXml(new File(file));

		System.out.println(x.getNodes().size());



	}
}