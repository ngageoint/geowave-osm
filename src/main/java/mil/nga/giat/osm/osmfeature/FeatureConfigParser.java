package mil.nga.giat.osm.osmfeature;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import mil.nga.giat.osm.osmfeature.types.attributes.AttributeDefinition;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinition;
import mil.nga.giat.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.osm.osmfeature.types.features.FeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class FeatureConfigParser
{
	private final static Logger LOGGER = LoggerFactory.getLogger(FeatureConfigParser.class);
	private final JsonFactory factory = new JsonFactory();

	public void parseConfig(InputStream configStream)
			throws IOException {


		ObjectMapper om = new ObjectMapper();

		JsonNode rootNode = om.readTree(configStream);

		JsonNode tables = rootNode.path("tables");

		Iterator<Map.Entry<String, JsonNode>> nodeIterator = tables.fields();
		while (nodeIterator.hasNext()){
			Map.Entry<String, JsonNode> feature = nodeIterator.next();
			FeatureDefinition fd = parseFeature(feature.getKey(), feature.getValue());
			FeatureDefinitionSet.Features.add(fd);
		}

	}

	private static FeatureDefinition parseFeature(String name, JsonNode node){
		FeatureDefinition fd = new FeatureDefinition();
		fd.Name = name;
		Iterator<Map.Entry<String, JsonNode>> featureIterator = node.fields();
		while (featureIterator.hasNext()){
			Map.Entry<String, JsonNode> props = featureIterator.next();
			switch (props.getKey()){
				case  "fields" : {
					parseFields(props.getValue(), fd);
					break;
				}
				case "type" : {
					switch (props.getValue().asText()){
						case "polygon" : {
							fd.Type = FeatureType.Polygon;
							break;
						}
						case "linestring" : {
							fd.Type = FeatureType.LineString;
							break;
						}
						case "point" : {
							fd.Type = FeatureType.Point;
							break;
						}
						case "geometry" : {
							fd.Type = FeatureType.Geometry;
							break;
						}
					}
					break;
				}
				case "mapping" : {
					parseMapping(props.getValue(), fd);
					break;
				}
				case "mappings" : {
					parseSubMappings(props.getValue(), fd);
					break;
				}
				case "filters" : {
					parseFilters(props.getValue(), fd);
					break;
				}
			}
		}
		return fd;
	}

	private static void parseFilters(JsonNode node, FeatureDefinition fd){
		Iterator<Map.Entry<String, JsonNode>> filterIter = node.fields();
		while (filterIter.hasNext()){
			Map.Entry<String, JsonNode> filterKVP = filterIter.next();
			Map<String, List<String>> filter = new HashMap<>();
			List<String> filterVals = new ArrayList<>();
			for (JsonNode filterVal : filterKVP.getValue()){
				filterVals.add(filterVal.asText());
			}
			filter.put(filterKVP.getKey(), filterVals);
			fd.Filters.add(filter);
		}
	}

	private static void parseMapping(JsonNode node, FeatureDefinition fd){
		Iterator<Map.Entry<String, JsonNode>> mappingIter = node.fields();
		while (mappingIter.hasNext()){
			Map.Entry<String, JsonNode> mapKVP = mappingIter.next();
			final List<String> mapValues = new ArrayList<>();
			for (JsonNode mapVal : mapKVP.getValue()){
				mapValues.add(mapVal.asText());
			}
			fd.Mappings.put(mapKVP.getKey(), mapValues);
			fd.MappingKeys.add(mapKVP.getKey());
		}
	}

	private static void parseSubMappings(JsonNode node, FeatureDefinition fd){
		Iterator<Map.Entry<String, JsonNode>> mappingsIter = node.fields();
		while (mappingsIter.hasNext()){
			Map.Entry<String, JsonNode> mappingsKVP = mappingsIter.next();
			for (JsonNode mapping : mappingsKVP.getValue()){
				Iterator<Map.Entry<String, JsonNode>> mappIter = mapping.fields();
				while (mappIter.hasNext()){
					Map.Entry<String, JsonNode> mappKVP = mappIter.next();
					final Map<String, List<String>> submapping = new HashMap<>();
					final List<String> submappingValues = new ArrayList<>();
					for (JsonNode subMapVal : mappKVP.getValue()){
						submappingValues.add(subMapVal.asText());
					}
					submapping.put(mappKVP.getKey(), submappingValues);
					if (!fd.SubMappings.containsKey(mappingsKVP.getKey())){
						fd.SubMappings.put(mappingsKVP.getKey(), new ArrayList<Map<String, List<String>>>());
					}
					fd.SubMappings.get(mappingsKVP.getKey()).add(submapping);
					fd.MappingKeys.add(mappingsKVP.getKey());
				}

			}

		}
	}


	private static void parseFields(JsonNode node, FeatureDefinition fd){
		for (JsonNode attr : node){
			Iterator<Map.Entry<String, JsonNode>> fieldIterator = attr.fields();
			final AttributeDefinition ad = new AttributeDefinition();
			while (fieldIterator.hasNext()){
				Map.Entry<String, JsonNode> field = fieldIterator.next();
				switch (field.getKey()){
					case "type" : {
						ad.Type = field.getValue().asText();
						break;
					}
					case "name" : {
						ad.Name = field.getValue().asText();
						break;
					}
					case "key" : {
						ad.Key = field.getValue().asText();
						break;
					}
					case "args" : {
						Iterator<Map.Entry<String, JsonNode>> argsIterator = field.getValue().fields();
						while (argsIterator.hasNext()){
							Map.Entry<String, JsonNode> arg = argsIterator.next();
							List<String> allArgs = new ArrayList<>();
							for (JsonNode item : arg.getValue()){
								allArgs.add(item.asText());
							}
							ad.Args.put(arg.getKey(), allArgs);
						}
						break;
					}
				}
			}
			fd.Attributes.add(ad);
		}

	}


}
