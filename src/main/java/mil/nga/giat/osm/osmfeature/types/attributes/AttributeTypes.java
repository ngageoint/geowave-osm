package mil.nga.giat.osm.osmfeature.types.attributes;

import com.vividsolutions.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeTypes
{
	private final static Map<Class, AttributeType> AttributeDefinitionCache = new HashMap<Class, AttributeType>();
	private final static Logger log = LoggerFactory.getLogger(AttributeTypes.class);

	static {
		AttributeDefinitionCache.put(String.class, new StringAttributeType());
		AttributeDefinitionCache.put(Double.class, new DoubleAttributeType());
		AttributeDefinitionCache.put(Long.class, new LongAttributeType());
		AttributeDefinitionCache.put(Integer.class, new IntegerAttributeType());
		AttributeDefinitionCache.put(Boolean.class, new BooleanAttributeType());
	}

	public static AttributeType getAttributeType(String imposm3TypeName){
		switch (imposm3TypeName){
			case "id" : {
				return AttributeDefinitionCache.get(Long.class);
			}
			case "osm_id" : {
				return AttributeDefinitionCache.get(Long.class);
			}
			case "string" : {
				return AttributeDefinitionCache.get(String.class);
			}
			case "pseudoarea" : {
				return AttributeDefinitionCache.get(Double.class);
			}
			case "zorder" : {
				return AttributeDefinitionCache.get(Short.class);
			}
			case "wayzorder" : {
				return AttributeDefinitionCache.get(Short.class);
			}
			case "mapping_value" : {
				return AttributeDefinitionCache.get(String.class);
			}
			case "boolint" : {
				return AttributeDefinitionCache.get(Boolean.class);
			}
			case "direction" : {
				return AttributeDefinitionCache.get(Short.class);
			}
			case "mapping_key" : {
				return AttributeDefinitionCache.get(String.class);
			}
		}
		return null;
	}



	private static class StringAttributeType implements AttributeType<String> {
		@Override
		public String convert( Object source ) {
			if (source == null){
				return null;
			}
			return String.valueOf(source);
		}

		@Override
		public Class getClassType() {
			return String.class;
		}
	}

	private static class DoubleAttributeType implements AttributeType<Double> {
		@Override
		public Double convert( Object source ) {
			if (source == null){
				return null;
			}
			if (source instanceof Double) {
				return (Double) source;
			}
			return Double.valueOf(source.toString());
		}

		@Override
		public Class getClassType() {
			return Double.class;
		}
	}

	private static class IntegerAttributeType implements AttributeType<Integer> {
		@Override
		public Integer convert( Object source ) {
			if (source == null){
				return null;
			}
			if (source instanceof Integer) {
				return (Integer) source;
			}
			return Integer.valueOf(source.toString());
		}

		@Override
		public Class getClassType() {
			return Integer.class;
		}
	}

	private static class LongAttributeType implements AttributeType<Long> {
		@Override
		public Long convert( Object source ) {
			if (source == null){
				return null;
			}
			if (source instanceof Long) {
				return (Long) source;
			}
			return Long.valueOf(source.toString());
		}

		@Override
		public Class getClassType() {
			return Long.class;
		}
	}


	private static class ShortAttributeType implements AttributeType<Short> {
		@Override
		public Short convert( Object source ) {
			if (source == null){
				return null;
			}
			if (source instanceof Short) {
				return (Short) source;
			}
			return Short.valueOf(String.valueOf(source));

		}

		@Override
		public Class getClassType() {
			return Short.class;
		}
	}

		private static class BooleanAttributeType implements AttributeType<Boolean> {
		@Override
		public Boolean convert( Object source ) {
			if (source == null){
				return null;
			}
			if (source instanceof Boolean) {
				return (Boolean) source;
			}
			String val = String.valueOf(source).toLowerCase().trim();

			if (val.equals("1") || val.equals("true") || val.equals("t") || val.equals("y") || val.equals("yes")) {
				return true;
			} else if (val.equals("0") || val.equals("false") || val.equals("f") || val.equals("n") || val.equals("no")){
				return false;
			}
			if (val ==null){
				val = "<NULL>";
			}
			log.error("Unable to parse value: " + val + " as boolean");
			throw new IllegalArgumentException("Value: " + val + " was not a valid boolean value");
		}

		@Override
		public Class getClassType() {
			return Boolean.class;
		}
	}


}
