package mil.nga.giat.osm.osmfeature.types.features;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.osm.osmfeature.types.attributes.AttributeDefinition;

public class FeatureDefinition {
	public String Name = null;
	public FeatureType Type = null;
	public final Map<String, List<String>> Mappings = new HashMap<>();
	public final Map<String, List<Map<String, List<String>>>> SubMappings = new HashMap<>();
	public final List<AttributeDefinition> Attributes = new ArrayList<>();
	public final List<Map<String, List<String>>> Filters = new ArrayList<>();
	public final List<String> MappingKeys = new ArrayList<>();
    private static final String WILDCARD_ATTRIBUTE = "__any__";

	public String getMappingName() {
		for (AttributeDefinition ad : Attributes){
			if (ad.Type.equals("mapping_value")){
				return ad.Name;
			}
		}
		return null;
	}

	public AttributeDefinition getMappingAttribute(){
		for (AttributeDefinition ad : Attributes){
			if (ad.Type.equals("mapping_value")){
				return ad;
			}
		}
		return null;
	}

	public String getQualifiedSubMappings(){
		for (AttributeDefinition ad : Attributes){
			if (ad.Type.equals("mapping_key")){
				return ad.Name;
			}
		}
		return null;
	}

	public AttributeDefinition getSubMappingAttribute(){
		for (AttributeDefinition ad : Attributes){
			if (ad.Type.equals("mapping_key")){
				return ad;
			}
		}
		return null;
	}

	public boolean isMappedValue(String val){
		for (Map.Entry<String, List<String>> map : Mappings.entrySet()){
			if (map.getValue().contains(WILDCARD_ATTRIBUTE) || map.getValue().contains(val)){
				return true;
			}
		}
		return false;
	}


	public String getSubMappingClass(String key, String val){
		for (Map.Entry<String, List<Map<String, List<String>>>> m : SubMappings.entrySet()){
			for (Map<String, List<String>> m2 : m.getValue()){
				for (Map.Entry<String, List<String>> m3 : m2.entrySet()){
					if (m3.getKey().equals(key)) {
						if (m3.getValue().contains(WILDCARD_ATTRIBUTE) || m3.getValue().contains(val)) {
							return m3.getKey();
						}
					}
				}
			}
		}
		return null;
	}




}
