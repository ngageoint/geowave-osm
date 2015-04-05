/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package mil.nga.giat.osm.types.generated;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Way extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Way\",\"namespace\":\"mil.nga.giat.osm.types.generated\",\"fields\":[{\"name\":\"common\",\"type\":{\"type\":\"record\",\"name\":\"Primitive\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"version\",\"type\":\"long\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"user_id\",\"type\":[\"null\",\"long\"]},{\"name\":\"user_name\",\"type\":[\"null\",\"string\"]},{\"name\":\"changeset_id\",\"type\":\"long\"},{\"name\":\"visible\",\"type\":\"boolean\",\"default\":\"true\"},{\"name\":\"tags\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}]}},{\"name\":\"nodes\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"long\"}]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public mil.nga.giat.osm.types.generated.Primitive common;
  @Deprecated public java.util.List<java.lang.Long> nodes;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Way() {}

  /**
   * All-args constructor.
   */
  public Way(mil.nga.giat.osm.types.generated.Primitive common, java.util.List<java.lang.Long> nodes) {
    this.common = common;
    this.nodes = nodes;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return common;
    case 1: return nodes;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: common = (mil.nga.giat.osm.types.generated.Primitive)value$; break;
    case 1: nodes = (java.util.List<java.lang.Long>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'common' field.
   */
  public mil.nga.giat.osm.types.generated.Primitive getCommon() {
    return common;
  }

  /**
   * Sets the value of the 'common' field.
   * @param value the value to set.
   */
  public void setCommon(mil.nga.giat.osm.types.generated.Primitive value) {
    this.common = value;
  }

  /**
   * Gets the value of the 'nodes' field.
   */
  public java.util.List<java.lang.Long> getNodes() {
    return nodes;
  }

  /**
   * Sets the value of the 'nodes' field.
   * @param value the value to set.
   */
  public void setNodes(java.util.List<java.lang.Long> value) {
    this.nodes = value;
  }

  /** Creates a new Way RecordBuilder */
  public static mil.nga.giat.osm.types.generated.Way.Builder newBuilder() {
    return new mil.nga.giat.osm.types.generated.Way.Builder();
  }
  
  /** Creates a new Way RecordBuilder by copying an existing Builder */
  public static mil.nga.giat.osm.types.generated.Way.Builder newBuilder(mil.nga.giat.osm.types.generated.Way.Builder other) {
    return new mil.nga.giat.osm.types.generated.Way.Builder(other);
  }
  
  /** Creates a new Way RecordBuilder by copying an existing Way instance */
  public static mil.nga.giat.osm.types.generated.Way.Builder newBuilder(mil.nga.giat.osm.types.generated.Way other) {
    return new mil.nga.giat.osm.types.generated.Way.Builder(other);
  }
  
  /**
   * RecordBuilder for Way instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Way>
    implements org.apache.avro.data.RecordBuilder<Way> {

    private mil.nga.giat.osm.types.generated.Primitive common;
    private java.util.List<java.lang.Long> nodes;

    /** Creates a new Builder */
    private Builder() {
      super(mil.nga.giat.osm.types.generated.Way.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(mil.nga.giat.osm.types.generated.Way.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.common)) {
        this.common = data().deepCopy(fields()[0].schema(), other.common);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.nodes)) {
        this.nodes = data().deepCopy(fields()[1].schema(), other.nodes);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Way instance */
    private Builder(mil.nga.giat.osm.types.generated.Way other) {
            super(mil.nga.giat.osm.types.generated.Way.SCHEMA$);
      if (isValidValue(fields()[0], other.common)) {
        this.common = data().deepCopy(fields()[0].schema(), other.common);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.nodes)) {
        this.nodes = data().deepCopy(fields()[1].schema(), other.nodes);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Primitive getCommon() {
      return common;
    }
    
    /** Sets the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Way.Builder setCommon(mil.nga.giat.osm.types.generated.Primitive value) {
      validate(fields()[0], value);
      this.common = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'common' field has been set */
    public boolean hasCommon() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Way.Builder clearCommon() {
      common = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'nodes' field */
    public java.util.List<java.lang.Long> getNodes() {
      return nodes;
    }
    
    /** Sets the value of the 'nodes' field */
    public mil.nga.giat.osm.types.generated.Way.Builder setNodes(java.util.List<java.lang.Long> value) {
      validate(fields()[1], value);
      this.nodes = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'nodes' field has been set */
    public boolean hasNodes() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'nodes' field */
    public mil.nga.giat.osm.types.generated.Way.Builder clearNodes() {
      nodes = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public Way build() {
      try {
        Way record = new Way();
        record.common = fieldSetFlags()[0] ? this.common : (mil.nga.giat.osm.types.generated.Primitive) defaultValue(fields()[0]);
        record.nodes = fieldSetFlags()[1] ? this.nodes : (java.util.List<java.lang.Long>) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
