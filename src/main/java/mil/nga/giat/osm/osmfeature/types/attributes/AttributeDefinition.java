package mil.nga.giat.osm.osmfeature.types.attributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeDefinition {
	public String Type = null;
	public String Name = null;
	public String Key = null;
	public final Map<String, List<String>> Args = new HashMap<>();

	public Object convert(Object obj){
		return AttributeTypes.getAttributeType(Type).convert(obj);
	}
}
