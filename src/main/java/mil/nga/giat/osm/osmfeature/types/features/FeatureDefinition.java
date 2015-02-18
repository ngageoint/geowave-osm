package mil.nga.giat.osm.osmfeature.types.features;

import mil.nga.giat.osm.osmfeature.types.attributes.AttributeDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureDefinition {
	public String Name = null;
	public FeatureType Type = null;
	public final Map<String, List<String>> Mappings = new HashMap<>();
	public final Map<String, List<Map<String, List<String>>>> SubMappings = new HashMap<>();
	public final List<AttributeDefinition> Attributes = new ArrayList<>();
	public final List<Map<String, List<String>>> Filters = new ArrayList<>();




}
