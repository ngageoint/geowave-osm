package mil.nga.giat.osm.osmfeature;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;





import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.vividsolutions.jts.geom.Geometry;


/***
 *Feature attributes from: 
 *http://wiki.openstreetmap.org/wiki/Map_Features
 */
public class OSMFeatures {
		
	private static Map<String, SimpleFeatureType> _features = new HashMap<String, SimpleFeatureType>();
		
	public enum FEATURES {
		AERIALWAY("aerialway"),
		AEROWAY("aeroway"),
		AMENITY("amenity"),
		BARRIER("barrier"),
		BOUNDARY("boundary"),
		BUILDING("building"),
		CRAFT("craft"),
		EMERGENCY("emergency"),
		GEOLOGICAL("geological"),
		HIGHWAY("highway"),
		HISTORIC("historic"),
		LANDUSE("landuse"),
		LEISURE("leisure"),
		MAN_MADE("man_made"),
		MILITARY("military"),
		NATURAL("natural"),
		OFFICE("office"),
		PLACES("place"),
		POWER("power"),
		PUBLIC_TRANSPORT("public_transportation"),
		RAILWAY("railway"),
		ROUTE("route"),
		SHOP("shop"),
		SPORT("sport"),
		TOURISM("tourism"),
		WATERWAY("waterway");
		
		private String text;
	
		FEATURES(String text){
			this.text = text;
		}
	
		public String getText() {
			return text;
		}
	
		public static FEATURES fromString(String text){
			 if (text != null) {
			      for (FEATURES f : FEATURES.values()) {
			        if (text.equalsIgnoreCase(f.text)) {
			          return f;
			        }
			      }
			    }
			 return null;
		}
	}
	
	public enum PROPERTIES {
		ADDRESSES("addresses"),
		ANNOTATION("annotation"),
		NAME("name"),
		PROPERTIES("properties"),
		REFERENCES("references"),
		RESTRICTIONS("restrictions");
		
		
		private String text;
		
		PROPERTIES(String text){
			this.text = text;
		}
	
		public String getText() {
			return text;
		}
	
		public static PROPERTIES fromString(String text){
			 if (text != null) {
			      for (PROPERTIES f : PROPERTIES.values()) {
			        if (text.equalsIgnoreCase(f.text)) {
			          return f;
			        }
			      }
			    }
			 return null;
		}
	}
	
	
	
	public static void OSMFeatures(){
				
		
	}
	
	//feature types
	private static final Map<String, Class> _aerialways = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("aerialway", String.class)
			.build());
	
	private static final Map<String, Class> _aeroways = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("aeroway",String.class)
			.build());
	
	private static final Map<String, Class> _amenities = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("amenity",String.class)
			.build());
		
	private static final Map<String, Class> _barrier = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("barrier",String.class)
			.build());
	
	private static final Map<String, Class> _boundary = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("boundary",String.class)
			.put("admin_level",Integer.class)
			.build());
	
	private static final Map<String, Class> _building = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("building",String.class)
			.put("entrance",String.class)
			.put("height", Float.class)
			.put("building-levels",Integer.class)
			.put("building-fireproof",Boolean.class)
			.build());
	
	private static final Map<String, Class> _craft = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("craft",String.class)
			.build());
	
	private static final Map<String, Class> _emergency = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("emergency",String.class)
			.build());
	
	private static final Map<String, Class> _geological = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("geological",String.class)
			.build());
	
	private static final Map<String, Class> _highway = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("highway",String.class)
			.put("sidewalk",String.class)
			.put("cycleway",String.class)
			.put("abutters",String.class)
			.put("ford",Boolean.class)
			.put("ice_road",Boolean.class)
			.put("incline",Double.class)
			.put("junction",String.class)
			.put("lanes",Integer.class)
			.put("lit",Boolean.class)
			.put("motorroad",Boolean.class)
			.put("mountain_pass",Boolean.class)
			.put("mtb-scale",Integer.class)
			.put("mtb-scale-uphill",Integer.class)
			.put("mtb-scale-imba",Integer.class)
			.put("mtb-description",String.class)
			.put("overtaking",String.class)
			.put("passing_places",Boolean.class)
			.put("sac_scale",String.class)
			.put("service",String.class)
			.put("surface",String.class)
			.put("tactile_paving",Boolean.class)
			.put("tracktype",String.class)
			.put("traffic_calming",String.class)
			.put("trail_visibility",String.class)
			.put("winter_road",String.class)
			.put("emergency",String.class)
			.build());
	
	private static final Map<String, Class> _historic = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("historic",String.class)
			.build());
	
	private static final Map<String, Class> _landuse = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("landuse",String.class)
			.build());
	
	private static final Map<String, Class> _leisure = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("leisure",String.class)
			.build());
	
	private static final Map<String, Class> _manmade = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("man_made",String.class)
			.build());
	
	private static final Map<String, Class> _military = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("military",String.class)
			.build());
	
	private static final Map<String, Class> _natural = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("natural",String.class)
			.build());
	
	private static final Map<String, Class> _office = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("office",String.class)
			.build());
	
	private static final Map<String, Class> _place = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("place",String.class)
			.put("population",Integer.class)
			.put("name",String.class)
			.put("place_numbers",Integer.class)
			.put("postal_code",String.class)
			.put("reference_point",Boolean.class)
			.put("is_in",String.class)
			.build());
	
	private static final Map<String, Class> _power = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("power",String.class)
			.put("cables",Integer.class)
			.put("circuits",Integer.class)
			.put("tunnel",Boolean.class)
			.put("voltage",Integer.class)
			.put("wires",String.class)
			.build());
	
	private static final Map<String, Class> _publicTransport = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("public_transport",String.class)
			.build());
	
	private static final Map<String, Class> _railway = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("railway",Boolean.class)
			.put("bridge",Boolean.class)
			.put("electrified",String.class)
			.put("embankment",Boolean.class)
			.put("frequency",Integer.class)
			.put("service",String.class)
			.put("tracks",Integer.class)
			.put("tunnel",Boolean.class)
			.put("usage",String.class)
			.put("voltage",Integer.class)
			.put("public_transport",String.class)
			.put("landuse",String.class)
			.build());
	
	private static final Map<String, Class> _route = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("route",String.class)
			.put("name",String.class)
			.put("ref",String.class)
			.put("network",String.class)
			.put("operator",String.class)
			.put("state",String.class)
			.put("symbol",String.class)
			.put("description",String.class)
			.put("distance",String.class)
			.put("ascent",String.class)
			.put("descent",String.class)
			.put("roundtrip",String.class)
			.put("colour",String.class)
			.build());
	
	private static final Map<String, Class> _shop = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("shop",String.class)
			.build());
	
	private static final Map<String, Class> _sport = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("sport",String.class)
			.build());
	
	private static final Map<String, Class> _tourism = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("tourism",String.class)
			.build());
	
	private static final Map<String, Class> _waterway = getBaseFeature(ImmutableMap.<String, Class>builder()
			.put("waterway",String.class)
			.put("intermittent",Boolean.class)
			.put("lock",Boolean.class)
			.put("mooring",String.class)
			.put("service",String.class)
			.put("tunnel",String.class)
			.build());
	
	
//Additional properties
	private static final Map<String, Class> _address = ImmutableMap.<String, Class>builder()
			.put("addr-housenumber",String.class)
			.put("addr-housename",String.class)
			.put("addr-street",String.class)
			.put("addr-place",String.class)
			.put("addr-postcode",String.class)
			.put("addr-city",String.class)
			.put("addr-country",String.class)
			.put("addr-full",String.class)
			.put("addr-hamlet",String.class)
			.put("addr-suburb",String.class)
			.put("addr-subdistrict",String.class)
			.put("addr-district",String.class)
			.put("addr-province",String.class)
			.put("addr-state",String.class)
			.put("addr-interpolation",String.class)
			.put("addr-inclusion",String.class)
			.build();
	
	
	private static final Map<String, Class> _annotation = ImmutableMap.<String, Class>builder()
			.put("attribution",String.class)
			.put("description",String.class)
			.put("email",String.class)
			.put("fax",String.class)
			.put("fixme",String.class)
			.put("image",String.class)
			.put("note",String.class)
			.put("phone",String.class)
			.put("source",String.class)
			.put("source-name",String.class)
			.put("source-ref",String.class) 
			.put("source_ref",String.class)
			.put("url",String.class)
			.put("website",String.class)
			.put("wikipedia",String.class)
			.build();
	
	private static final Map<String, Class> _name = ImmutableMap.<String, Class>builder()
			.put("name",String.class)
			.put("name-",String.class) //language support - e.g. name-fr, name-sp
			.put("alt_name",String.class)
			.put("alt_name-",String.class) //language support
			.put("int_name",String.class)
			.put("loc_name",String.class)
			.put("nat_name",String.class)
			.put("official_name",String.class)
			.put("old_name",String.class)
			.put("old_name-",String.class) //language support
			.put("reg_name",String.class)
			.put("short_name",String.class)
			.put("sorting_name",String.class)
			.build();
	
	private static final Map<String, Class> _property = ImmutableMap.<String, Class>builder()
			.put("area",Boolean.class)
			.put("bridge",String.class)
			.put("covered",Boolean.class)
			.put("crossing",String.class)
			.put("cutting",Boolean.class)
			.put("disused",Boolean.class)
			.put("drive_through",Boolean.class)
			.put("drive_in",Boolean.class)
			.put("ele",Double.class)
			.put("embankment",Boolean.class)
			.put("end_date",Date.class)
			.put("est_width",Double.class)
			.put("internet_access",String.class)
			.put("layer",Integer.class)
			.put("narrow",Boolean.class)
			.put("opening_hours",String.class)
			.put("operator",String.class)
			.put("start_date",Date.class)
			.put("TMC-LocationCode",String.class)
			.put("tunnel",Boolean.class)
			.put("toilets-wheelchair",String.class)
			.put("width",Double.class)
			.put("wood",String.class)
			.build();
	
	
	private static final Map<String, Class> _references = ImmutableMap.<String, Class>builder()
			.put("iata",Boolean.class)
			.put("icao",String.class)
			.put("int_ref",Boolean.class)
			.put("lcn_ref",Boolean.class)
			.put("loc_ref",String.class)
			.put("nat_ref",Boolean.class)
			.put("ncn_ref",Boolean.class)
			.put("old_ref",Boolean.class)
			.put("rcn_ref",Boolean.class)
			.put("ref",Boolean.class)
			.put("reg_ref",Boolean.class)
			.put("source_ref",Boolean.class)
			.build();
	
	private static final Map<String, Class> _restriction = ImmutableMap.<String, Class>builder()
			.put("access",String.class)
			.put("agricultural",Boolean.class)
			.put("atv",String.class)
			.put("bdouble",String.class)
			.put("bicycle",String.class)
			.put("boat",String.class)
			.put("emergency",Boolean.class)
			.put("foot",String.class)
			.put("forestry",Boolean.class)
			.put("goods",String.class)
			.put("hazmat",String.class)
			.put("hgv",String.class)
			.put("horse",String.class)
			.put("inline_skates",Boolean.class)
			.put("lhv",String.class)
			.put("motorboat",String.class)
			.put("motorcar",String.class)
			.put("motorcycle",String.class)
			.put("motor_vehicle",String.class)
			.put("psv",String.class)
			.put("roadtrain",String.class)
			.put("tank",String.class)
			.put("vehicle",String.class)
			.put("4wd_only",Boolean.class)
			.put("charge",Double.class)
			.put("disused",Boolean.class)
			.put("maxheight",String.class)
			.put("maxlength",String.class)
			.put("maxspeed",String.class)
			.put("maxstay",Integer.class)
			.put("maxweight",String.class)
			.put("maxwidth",String.class)
			.put("minspeed",String.class)
			.put("noexit",Boolean.class)
			.put("oneway",String.class)
			.put("toll-key",Boolean.class)
			.put("traffic_sign",String.class)
			.build();
	
	
		
	private static  Map<String, Class> getBaseFeature(ImmutableMap<String, Class> newAttributes) {
		
		Builder<String, Class> builder = new Builder<String, Class>();
		
		//common attributes
		builder
			.put("geom",Geometry.class)
			.put("id",Long.class)
			.put("feature_name",String.class)
			.put("type",String.class);
		
		if (newAttributes != null){
			for (Entry<String, Class> kvp : newAttributes.entrySet()) {
				builder.put(kvp);
			}
		}
		return builder.build();
	}
	
		
	private static SimpleFeatureType featureBuilder(String name, Map<String, Class> types){
		
		if (!_features.containsKey(name)){
			final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
			simpleFeatureTypeBuilder.setName(name);
	
			final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
	
			for (Entry<String, Class> kvp : types.entrySet()){
				simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
			}
			
			
			for (Entry<String, Class> kvp : _annotation.entrySet()){
				if (!types.keySet().contains(kvp.getKey())){
					simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
				}
				
			}
			
			for (Entry<String, Class> kvp : _name.entrySet()){
				if (!types.keySet().contains(kvp.getKey())){
					simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
				}
				
			}
			
			
			for (Entry<String, Class> kvp : _property.entrySet()){
				if (!types.keySet().contains(kvp.getKey())){
					simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
				}
				
			}
			
			for (Entry<String, Class> kvp : _references.entrySet()){
				if (!types.keySet().contains(kvp.getKey())){
					simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
				}
				
			}
		
			for (Entry<String, Class> kvp : _restriction.entrySet()){
				if (!types.keySet().contains(kvp.getKey())){
					simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
				}
				
			}
			_features.put(name,simpleFeatureTypeBuilder.buildFeatureType());
		}
		
		return _features.get(name);
	}
	
	public static SimpleFeatureType GetFeature(String feat, String geomPrefix){
		return GetFeature(FEATURES.fromString(feat), geomPrefix);
	}
	
	
	
	
	public static SimpleFeatureType GetFeatureGeom(String geomPrefix){
		
		if (!_features.containsKey(geomPrefix.toLowerCase())){
			
			Map<String, Class> baseFeatures = getBaseFeature(null);
			
			Map<String, Class>[] features = new Map[] {baseFeatures, _aerialways,_aeroways,_amenities,_barrier,_boundary,_building,_craft,_emergency,_geological,_highway,_historic,_landuse,_leisure,_manmade,_military,_natural,_office,_place,_power,_publicTransport,_railway,_route,_shop,_sport,_tourism,_waterway,_address,_annotation,_name,_property,_references,_restriction};
			final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
			simpleFeatureTypeBuilder.setName(geomPrefix.toLowerCase());
	
			final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
	
			List<String> usedAttributes = new ArrayList<String>();
			
			for (Map<String, Class> types : features){
				for (Entry<String, Class> kvp : types.entrySet()){
					if (!usedAttributes.contains(kvp.getKey())){
						simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(kvp.getValue()).nillable(true).buildDescriptor(kvp.getKey()));
						usedAttributes.add(kvp.getKey());
					}
				}
			}
			
			_features.put(geomPrefix.toLowerCase(),simpleFeatureTypeBuilder.buildFeatureType());
		}
		
		return _features.get(geomPrefix.toLowerCase());
	}
	
	
	
	
	public static SimpleFeatureType GetFeature(FEATURES feat, String geomPrefix){
		
		if (feat == null)
			return null;
		
		String name = feat.getText() + "_" + geomPrefix.toLowerCase();
		
		switch (feat){
			case AERIALWAY:
				return featureBuilder(name + "_", _aerialways);
			case AMENITY:
				return featureBuilder(name,_amenities);
			case AEROWAY:
				return featureBuilder(name,_aeroways);
			case BARRIER:
				return featureBuilder(name,_barrier);
			case BOUNDARY:
				return featureBuilder(name,_boundary);
			case BUILDING:
				return featureBuilder(name,_building);
			case CRAFT:
				return featureBuilder(name,_craft);
			case EMERGENCY:
				return featureBuilder(name,_emergency);
			case GEOLOGICAL:
				return featureBuilder(name,_geological);
			case HIGHWAY:
				return featureBuilder(name,_highway);
			case HISTORIC:
				return featureBuilder(name,_historic);
			case LANDUSE:
				return featureBuilder(name,_landuse);
			case LEISURE:
				return featureBuilder(name,_leisure);
			case MAN_MADE:
				return featureBuilder(name,_manmade);
			case MILITARY:
				return featureBuilder(name,_military);
			case NATURAL:
				return featureBuilder(name,_natural);
			case OFFICE:
				return featureBuilder(name,_office);
			case PLACES:
				return featureBuilder(name,_place);
			case POWER:
				return featureBuilder(name,_power);
			case PUBLIC_TRANSPORT:
				return featureBuilder(name,_publicTransport);
			case RAILWAY:
				return featureBuilder(name,_railway);
			case ROUTE:
				return featureBuilder(name,_route);
			case SHOP:
				return featureBuilder(name,_shop);
			case SPORT:
				return featureBuilder(name,_sport);
			case TOURISM:
				return featureBuilder(name,_tourism);
			case WATERWAY:
				return featureBuilder(name,_waterway);
		}
		return null;
	}
	
	public static SimpleFeatureType GetProperties(PROPERTIES prop, String geomPrefix){
	
		
		String name = prop.getText() + "_" + geomPrefix.toLowerCase();
		
		switch (prop){
			case ADDRESSES:
				return featureBuilder(name, _address);
			case ANNOTATION:
				return featureBuilder(name, _annotation);
			case NAME:
				return featureBuilder(name, _name);
			case PROPERTIES:
				return featureBuilder(name, _property);
			case REFERENCES:
				return featureBuilder(name, _references);
			case RESTRICTIONS:
				return featureBuilder(name, _restriction);
		}
		return null;
	}
	

	
	
}
