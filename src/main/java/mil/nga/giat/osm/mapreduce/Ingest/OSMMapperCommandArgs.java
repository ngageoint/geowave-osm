package mil.nga.giat.osm.mapreduce.Ingest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

public class OSMMapperCommandArgs {

    public OSMMapperCommandArgs(){}

    public OSMMapperCommandArgs(String zookeepers, String instanceName, String user, String pass,
                                String osmNamespace, String visibility, String hdfsSequenceFile,
                                String jobName, String mapperType){
        this.zookeepers = zookeepers;
        this.instanceName = instanceName;
        this.user = user;
        this.pass = pass;
        this.osmNamespace = osmNamespace;
        this.visibility = visibility;
        this.hdfsBasePath = hdfsSequenceFile;
        this.jobName = jobName;
        this.mapperType = mapperType;
    }

	private String separator = "|||";

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

    @Parameter(names = {"-out", "--hdfsBasePath"}, required = false, description = "file to stage hdfs files to  - user must have write permissions")
	public String hdfsBasePath = "/user/" + System.getProperty("user.name") + "/osm_stage/";

    @Parameter(names = {"-jn", "--jobName"}, required = false, description = "Name of mapreduce job")
	public String jobName = "Ingest (" + System.getProperty("user.name") + ")";

    @Parameter(names = {"-t", "--type"}, required = true, description = "Mapper type - one of node, way, or relation")
    public String mapperType;

	@Parameter(names = {"-m", "--mappingFile"}, required = false, description = "Mapping file, imposm3 form")
	public String mappingFile = null;

    protected String osmTableName = "OSM";

	public String getQualifiedTableName(){
        return AccumuloUtils.getQualifiedTableName(osmNamespace,osmTableName);
    }

	public String getNodesBasePath(){
		return hdfsBasePath + "/nodes";
	}

	public String getWaysBasePath(){
		return hdfsBasePath + "/ways";
	}

	public String getRelationsBasePath(){
		return hdfsBasePath + "/relations";
	}

	public void processMappingFile()
			throws IOException {
		if (mappingFile != null){
			File f = new File(mappingFile);
			if (f.exists()){
				mappingContents = new String(Files.readAllBytes(Paths.get(mappingFile)));
			}
		}
	}

	public void setMappingContents(String content){
		mappingContents = content;
	}

	public String getMappingContents(){
		return mappingContents;
	}


	public String serializeToString(){
		StringBuilder sb = new StringBuilder();
		sb.append(zookeepers).append(separator).append(instanceName).append(separator).append(user).append(separator).append(pass).append(separator)
				.append(osmNamespace).append(separator).append(visibility).append(separator).append(hdfsBasePath).append(separator).append(jobName)
				.append(separator).append(mapperType);
		return sb.toString();
	}

	public void deserializeFromString(String ser){
		String[] settings = ser.split(Pattern.quote(separator));
		zookeepers = settings[0];
		instanceName = settings[1];
		user = settings[2];
		pass = settings[3];
		osmNamespace  = settings[4];
		visibility = settings[5];
		hdfsBasePath = settings[6];
		jobName = settings[7];
		mapperType = settings[8];
	}



	//This the imposm3 "test_mapping.json" file
	private String mappingContents = "{\n" + "  \"generalized_tables\": {\n" + "    \"waterareas_gen1\": {\n" + "      \"source\": \"waterareas\",\n" + "      \"sql_filter\": \"ST_Area(geometry)>50000.000000\",\n" + "      \"tolerance\": 50.0\n" + "    },\n" + "    \"waterareas_gen0\": {\n" + "      \"source\": \"waterareas_gen1\",\n" + "      \"sql_filter\": \"ST_Area(geometry)>500000.000000\",\n" + "      \"tolerance\": 200.0\n" + "    },\n" + "    \"roads_gen0\": {\n" + "      \"source\": \"roads_gen1\",\n" + "      \"sql_filter\": null,\n" + "      \"tolerance\": 200.0\n" + "    },\n" + "    \"roads_gen1\": {\n" + "      \"source\": \"roads\",\n" + "      \"sql_filter\": \"type IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link') OR class IN('railway')\",\n" + "      \"tolerance\": 50.0\n" + "    },\n" + "    \"waterways_gen0\": {\n" + "      \"source\": \"waterways_gen1\",\n"
			+ "      \"sql_filter\": null,\n" + "      \"tolerance\": 200\n" + "    },\n" + "    \"waterways_gen1\": {\n" + "      \"source\": \"waterways\",\n" + "      \"sql_filter\": null,\n" + "      \"tolerance\": 50.0\n" + "    },\n" + "    \"landusages_gen1\": {\n" + "      \"source\": \"landusages\",\n" + "      \"sql_filter\": \"ST_Area(geometry)>50000.000000\",\n" + "      \"tolerance\": 50.0\n" + "    },\n" + "    \"landusages_gen0\": {\n" + "      \"source\": \"landusages_gen1\",\n" + "      \"sql_filter\": \"ST_Area(geometry)>500000.000000\",\n" + "      \"tolerance\": 200.0\n" + "    }\n" + "  },\n" + "  \"tables\": {\n" + "    \"landusages\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"validated_geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n"
			+ "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"pseudoarea\",\n" + "          \"name\": \"area\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"args\": {\n" + "            \"ranks\": [\n" + "              \"pedestrian\",\n" + "              \"footway\",\n" + "              \"playground\",\n" + "              \"park\",\n" + "              \"forest\",\n" + "              \"cemetery\",\n" + "              \"farmyard\",\n" + "              \"farm\",\n" + "              \"farmland\",\n" + "              \"wood\",\n" + "              \"meadow\",\n" + "              \"grass\",\n" + "              \"wetland\",\n" + "              \"village_green\",\n"
			+ "              \"recreation_ground\",\n" + "              \"garden\",\n" + "              \"sports_centre\",\n" + "              \"pitch\",\n" + "              \"common\",\n" + "              \"allotments\",\n" + "              \"golf_course\",\n" + "              \"university\",\n" + "              \"school\",\n" + "              \"college\",\n" + "              \"library\",\n" + "              \"baracks\",\n" + "              \"fuel\",\n" + "              \"parking\",\n" + "              \"nature_reserve\",\n" + "              \"cinema\",\n" + "              \"theatre\",\n" + "              \"place_of_worship\",\n" + "              \"hospital\",\n" + "              \"scrub\",\n" + "              \"orchard\",\n" + "              \"vineyard\",\n" + "              \"zoo\",\n" + "              \"quarry\",\n" + "              \"residential\",\n" + "              \"retail\",\n" + "              \"commercial\",\n" + "              \"industrial\",\n"
			+ "              \"railway\",\n" + "              \"heath\",\n" + "              \"island\",\n" + "              \"land\"\n" + "            ]\n" + "          },\n" + "          \"type\": \"zorder\",\n" + "          \"name\": \"z_order\",\n" + "          \"key\": \"z_order\"\n" + "        }\n" + "      ],\n" + "      \"type\": \"polygon\",\n" + "      \"mapping\": {\n" + "        \"amenity\": [\n" + "          \"university\",\n" + "          \"school\",\n" + "          \"college\",\n" + "          \"library\",\n" + "          \"fuel\",\n" + "          \"parking\",\n" + "          \"cinema\",\n" + "          \"theatre\",\n" + "          \"place_of_worship\",\n" + "          \"hospital\"\n" + "        ],\n" + "        \"barrier\": [\n" + "          \"hedge\"\n" + "        ],\n" + "        \"leisure\": [\n" + "          \"park\",\n" + "          \"garden\",\n" + "          \"playground\",\n" + "          \"golf_course\",\n" + "          \"sports_centre\",\n"
			+ "          \"pitch\",\n" + "          \"stadium\",\n" + "          \"common\",\n" + "          \"nature_reserve\"\n" + "        ],\n" + "        \"tourism\": [\n" + "          \"zoo\"\n" + "        ],\n" + "        \"natural\": [\n" + "          \"wood\",\n" + "          \"land\",\n" + "          \"scrub\",\n" + "          \"wetland\",\n" + "          \"heath\"\n" + "        ],\n" + "        \"man_made\": [\n" + "          \"pier\"\n" + "        ],\n" + "        \"aeroway\": [\n" + "          \"runway\",\n" + "          \"taxiway\"\n" + "        ],\n" + "        \"place\": [\n" + "          \"island\"\n" + "        ],\n" + "        \"military\": [\n" + "          \"barracks\"\n" + "        ],\n" + "        \"landuse\": [\n" + "          \"park\",\n" + "          \"forest\",\n" + "          \"residential\",\n" + "          \"retail\",\n" + "          \"commercial\",\n" + "          \"industrial\",\n" + "          \"railway\",\n" + "          \"cemetery\",\n"
			+ "          \"grass\",\n" + "          \"farmyard\",\n" + "          \"farm\",\n" + "          \"farmland\",\n" + "          \"orchard\",\n" + "          \"vineyard\",\n" + "          \"wood\",\n" + "          \"meadow\",\n" + "          \"village_green\",\n" + "          \"recreation_ground\",\n" + "          \"allotments\",\n" + "          \"quarry\"\n" + "        ],\n" + "        \"highway\": [\n" + "          \"pedestrian\",\n" + "          \"footway\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"buildings\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n"
			+ "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"polygon\",\n" + "      \"mapping\": {\n" + "        \"building\": [\n" + "          \"__any__\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"places\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"args\": {\n"
			+ "            \"ranks\": [\n" + "              \"country\",\n" + "              \"state\",\n" + "              \"region\",\n" + "              \"county\",\n" + "              \"city\",\n" + "              \"town\",\n" + "              \"village\",\n" + "              \"hamlet\",\n" + "              \"suburb\",\n" + "              \"locality\"\n" + "            ]\n" + "          },\n" + "          \"type\": \"zorder\",\n" + "          \"name\": \"z_order\",\n" + "          \"key\": \"z_order\"\n" + "        },\n" + "        {\n" + "          \"type\": \"integer\",\n" + "          \"name\": \"population\",\n" + "          \"key\": \"population\"\n" + "        }\n" + "      ],\n" + "      \"type\": \"point\",\n" + "      \"mapping\": {\n" + "        \"place\": [\n" + "          \"country\",\n" + "          \"state\",\n" + "          \"region\",\n" + "          \"county\",\n" + "          \"city\",\n" + "          \"town\",\n" + "          \"village\",\n"
			+ "          \"hamlet\",\n" + "          \"suburb\",\n" + "          \"locality\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"transport_areas\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"polygon\",\n" + "      \"mapping\": {\n" + "        \"railway\": [\n" + "          \"station\",\n" + "          \"platform\"\n" + "        ],\n" + "        \"aeroway\": [\n"
			+ "          \"aerodrome\",\n" + "          \"terminal\",\n" + "          \"helipad\",\n" + "          \"apron\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"admin\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"integer\",\n" + "          \"name\": \"admin_level\",\n" + "          \"key\": \"admin_level\"\n" + "        }\n" + "      ],\n" + "      \"type\": \"polygon\",\n"
			+ "      \"mapping\": {\n" + "        \"boundary\": [\n" + "          \"administrative\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"aeroways\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"linestring\",\n" + "      \"mapping\": {\n" + "        \"aeroway\": [\n" + "          \"runway\",\n" + "          \"taxiway\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"waterways\": {\n"
			+ "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"linestring\",\n" + "      \"mapping\": {\n" + "        \"waterway\": [\n" + "          \"stream\",\n" + "          \"river\",\n" + "          \"canal\",\n" + "          \"drain\",\n" + "          \"ditch\"\n" + "        ],\n" + "        \"barrier\": [\n" + "          \"ditch\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"barrierways\": {\n"
			+ "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"linestring\",\n" + "      \"mapping\": {\n" + "        \"barrier\": [\n" + "          \"city_wall\",\n" + "          \"fence\",\n" + "          \"hedge\",\n" + "          \"retaining_wall\",\n" + "          \"wall\",\n" + "          \"bollard\",\n" + "          \"gate\",\n" + "          \"spikes\",\n" + "          \"lift_gate\",\n"
			+ "          \"kissing_gate\",\n" + "          \"embankment\",\n" + "          \"yes\",\n" + "          \"wire_fence\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"transport_points\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"ref\",\n" + "          \"key\": \"ref\"\n" + "        }\n" + "      ],\n" + "      \"type\": \"point\",\n"
			+ "      \"mapping\": {\n" + "        \"railway\": [\n" + "          \"station\",\n" + "          \"halt\",\n" + "          \"tram_stop\",\n" + "          \"crossing\",\n" + "          \"level_crossing\",\n" + "          \"subway_entrance\"\n" + "        ],\n" + "        \"aeroway\": [\n" + "          \"aerodrome\",\n" + "          \"terminal\",\n" + "          \"helipad\",\n" + "          \"gate\"\n" + "        ],\n" + "        \"highway\": [\n" + "          \"motorway_junction\",\n" + "          \"turning_circle\",\n" + "          \"bus_stop\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"amenities\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n"
			+ "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"point\",\n" + "      \"mapping\": {\n" + "        \"amenity\": [\n" + "          \"university\",\n" + "          \"school\",\n" + "          \"library\",\n" + "          \"fuel\",\n" + "          \"hospital\",\n" + "          \"fire_station\",\n" + "          \"police\",\n" + "          \"townhall\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"barrierpoints\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n"
			+ "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"point\",\n" + "      \"mapping\": {\n" + "        \"barrier\": [\n" + "          \"block\",\n" + "          \"bollard\",\n" + "          \"cattle_grid\",\n" + "          \"chain\",\n" + "          \"cycle_barrier\",\n" + "          \"entrance\",\n" + "          \"horse_stile\",\n" + "          \"gate\",\n" + "          \"spikes\",\n" + "          \"lift_gate\",\n" + "          \"kissing_gate\",\n" + "          \"fence\",\n" + "          \"yes\",\n" + "          \"wire_fence\",\n" + "          \"toll_booth\",\n" + "          \"stile\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"housenumbers_interpolated\": {\n" + "      \"fields\": [\n" + "        {\n"
			+ "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:street\",\n" + "          \"key\": \"addr:street\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:postcode\",\n" + "          \"key\": \"addr:postcode\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:city\",\n"
			+ "          \"key\": \"addr:city\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:inclusion\",\n" + "          \"key\": \"addr:inclusion\"\n" + "        }\n" + "      ],\n" + "      \"type\": \"linestring\",\n" + "      \"mapping\": {\n" + "        \"addr:interpolation\": [\n" + "          \"__any__\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"roads\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n"
			+ "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name:de\",\n" + "          \"key\": \"name:de\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"#\": \" check for different name/keys\",\n" + "          \"name\": \"name_en\",\n" + "          \"key\": \"name:en\"\n" + "        },\n" + "        {\n" + "          \"type\": \"boolint\",\n" + "          \"name\": \"tunnel\",\n" + "          \"key\": \"tunnel\"\n" + "        },\n" + "        {\n" + "          \"type\": \"boolint\",\n" + "          \"name\": \"bridge\",\n" + "          \"key\": \"bridge\"\n" + "        },\n" + "        {\n" + "          \"type\": \"direction\",\n" + "          \"name\": \"oneway\",\n" + "          \"key\": \"oneway\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"ref\",\n" + "          \"key\": \"ref\"\n" + "        },\n"
			+ "        {\n" + "          \"type\": \"wayzorder\",\n" + "          \"name\": \"z_order\",\n" + "          \"key\": \"layer\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"access\",\n" + "          \"key\": \"access\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"service\",\n" + "          \"key\": \"service\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_key\",\n" + "          \"name\": \"class\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"linestring\",\n" + "      \"filters\": {\n" + "        \"exclude_tags\": [\n" + "          [\"area\", \"yes\"]\n" + "        ]\n" + "      },\n" + "      \"mappings\": {\n" + "        \"railway\": {\n" + "          \"mapping\": {\n" + "            \"railway\": [\n" + "              \"rail\",\n" + "              \"tram\",\n" + "              \"light_rail\",\n"
			+ "              \"subway\",\n" + "              \"narrow_gauge\",\n" + "              \"preserved\",\n" + "              \"funicular\",\n" + "              \"monorail\",\n" + "              \"disused\"\n" + "            ]\n" + "          }\n" + "        },\n" + "        \"roads\": {\n" + "          \"mapping\": {\n" + "            \"man_made\": [\n" + "              \"pier\",\n" + "              \"groyne\"\n" + "            ],\n" + "            \"highway\": [\n" + "              \"motorway\",\n" + "              \"motorway_link\",\n" + "              \"trunk\",\n" + "              \"trunk_link\",\n" + "              \"primary\",\n" + "              \"primary_link\",\n" + "              \"secondary\",\n" + "              \"secondary_link\",\n" + "              \"tertiary\",\n" + "              \"tertiary_link\",\n" + "              \"road\",\n" + "              \"path\",\n" + "              \"track\",\n" + "              \"service\",\n" + "              \"footway\",\n"
			+ "              \"bridleway\",\n" + "              \"cycleway\",\n" + "              \"steps\",\n" + "              \"pedestrian\",\n" + "              \"living_street\",\n" + "              \"unclassified\",\n" + "              \"residential\",\n" + "              \"raceway\"\n" + "            ]\n" + "          }\n" + "        }\n" + "      }\n" + "    },\n" + "    \"housenumbers\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n"
			+ "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:street\",\n" + "          \"key\": \"addr:street\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:postcode\",\n" + "          \"key\": \"addr:postcode\"\n" + "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"addr:city\",\n" + "          \"key\": \"addr:city\"\n" + "        }\n" + "      ],\n" + "      \"type\": \"point\",\n" + "      \"mapping\": {\n" + "        \"addr:housenumber\": [\n" + "          \"__any__\"\n" + "        ]\n" + "      }\n" + "    },\n" + "    \"waterareas\": {\n" + "      \"fields\": [\n" + "        {\n" + "          \"type\": \"id\",\n" + "          \"name\": \"osm_id\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"validated_geometry\",\n" + "          \"name\": \"geometry\",\n" + "          \"key\": null\n"
			+ "        },\n" + "        {\n" + "          \"type\": \"string\",\n" + "          \"name\": \"name\",\n" + "          \"key\": \"name\"\n" + "        },\n" + "        {\n" + "          \"type\": \"mapping_value\",\n" + "          \"name\": \"type\",\n" + "          \"key\": null\n" + "        },\n" + "        {\n" + "          \"type\": \"pseudoarea\",\n" + "          \"name\": \"area\",\n" + "          \"key\": null\n" + "        }\n" + "      ],\n" + "      \"type\": \"polygon\",\n" + "      \"mapping\": {\n" + "        \"waterway\": [\n" + "          \"riverbank\"\n" + "        ],\n" + "        \"landuse\": [\n" + "          \"basin\",\n" + "          \"reservoir\"\n" + "        ],\n" + "        \"natural\": [\n" + "          \"water\"\n" + "        ],\n" + "        \"amenity\": [\n" + "          \"swimming_pool\"\n" + "        ],\n" + "        \"leisure\": [\n" + "          \"swimming_pool\"\n" + "        ]\n" + "      }\n" + "    }\n" + "  }\n" + "}";

}
