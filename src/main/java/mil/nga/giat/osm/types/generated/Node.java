/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package mil.nga.giat.osm.types.generated;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Node extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Node\",\"namespace\":\"mil.nga.giat.osm.types.generated\",\"fields\":[{\"name\":\"common\",\"type\":{\"type\":\"record\",\"name\":\"Primitive\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"version\",\"type\":\"long\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"user_id\",\"type\":[\"null\",\"long\"]},{\"name\":\"user_name\",\"type\":[\"null\",\"string\"]},{\"name\":\"changeset_id\",\"type\":\"long\"},{\"name\":\"visible\",\"type\":\"boolean\",\"default\":\"true\"},{\"name\":\"tags\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}]}},{\"name\":\"latitude\",\"type\":\"double\"},{\"name\":\"longitude\",\"type\":\"double\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public mil.nga.giat.osm.types.generated.Primitive common;
  @Deprecated public double latitude;
  @Deprecated public double longitude;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Node() {}

  /**
   * All-args constructor.
   */
  public Node(mil.nga.giat.osm.types.generated.Primitive common, java.lang.Double latitude, java.lang.Double longitude) {
    this.common = common;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return common;
    case 1: return latitude;
    case 2: return longitude;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: common = (mil.nga.giat.osm.types.generated.Primitive)value$; break;
    case 1: latitude = (java.lang.Double)value$; break;
    case 2: longitude = (java.lang.Double)value$; break;
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
   * Gets the value of the 'latitude' field.
   */
  public java.lang.Double getLatitude() {
    return latitude;
  }

  /**
   * Sets the value of the 'latitude' field.
   * @param value the value to set.
   */
  public void setLatitude(java.lang.Double value) {
    this.latitude = value;
  }

  /**
   * Gets the value of the 'longitude' field.
   */
  public java.lang.Double getLongitude() {
    return longitude;
  }

  /**
   * Sets the value of the 'longitude' field.
   * @param value the value to set.
   */
  public void setLongitude(java.lang.Double value) {
    this.longitude = value;
  }

  /** Creates a new Node RecordBuilder */
  public static mil.nga.giat.osm.types.generated.Node.Builder newBuilder() {
    return new mil.nga.giat.osm.types.generated.Node.Builder();
  }
  
  /** Creates a new Node RecordBuilder by copying an existing Builder */
  public static mil.nga.giat.osm.types.generated.Node.Builder newBuilder(mil.nga.giat.osm.types.generated.Node.Builder other) {
    return new mil.nga.giat.osm.types.generated.Node.Builder(other);
  }
  
  /** Creates a new Node RecordBuilder by copying an existing Node instance */
  public static mil.nga.giat.osm.types.generated.Node.Builder newBuilder(mil.nga.giat.osm.types.generated.Node other) {
    return new mil.nga.giat.osm.types.generated.Node.Builder(other);
  }
  
  /**
   * RecordBuilder for Node instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Node>
    implements org.apache.avro.data.RecordBuilder<Node> {

    private mil.nga.giat.osm.types.generated.Primitive common;
    private double latitude;
    private double longitude;

    /** Creates a new Builder */
    private Builder() {
      super(mil.nga.giat.osm.types.generated.Node.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(mil.nga.giat.osm.types.generated.Node.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.common)) {
        this.common = data().deepCopy(fields()[0].schema(), other.common);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.latitude)) {
        this.latitude = data().deepCopy(fields()[1].schema(), other.latitude);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.longitude)) {
        this.longitude = data().deepCopy(fields()[2].schema(), other.longitude);
        fieldSetFlags()[2] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Node instance */
    private Builder(mil.nga.giat.osm.types.generated.Node other) {
            super(mil.nga.giat.osm.types.generated.Node.SCHEMA$);
      if (isValidValue(fields()[0], other.common)) {
        this.common = data().deepCopy(fields()[0].schema(), other.common);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.latitude)) {
        this.latitude = data().deepCopy(fields()[1].schema(), other.latitude);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.longitude)) {
        this.longitude = data().deepCopy(fields()[2].schema(), other.longitude);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Primitive getCommon() {
      return common;
    }
    
    /** Sets the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Node.Builder setCommon(mil.nga.giat.osm.types.generated.Primitive value) {
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
    public mil.nga.giat.osm.types.generated.Node.Builder clearCommon() {
      common = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'latitude' field */
    public java.lang.Double getLatitude() {
      return latitude;
    }
    
    /** Sets the value of the 'latitude' field */
    public mil.nga.giat.osm.types.generated.Node.Builder setLatitude(double value) {
      validate(fields()[1], value);
      this.latitude = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'latitude' field has been set */
    public boolean hasLatitude() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'latitude' field */
    public mil.nga.giat.osm.types.generated.Node.Builder clearLatitude() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'longitude' field */
    public java.lang.Double getLongitude() {
      return longitude;
    }
    
    /** Sets the value of the 'longitude' field */
    public mil.nga.giat.osm.types.generated.Node.Builder setLongitude(double value) {
      validate(fields()[2], value);
      this.longitude = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'longitude' field has been set */
    public boolean hasLongitude() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'longitude' field */
    public mil.nga.giat.osm.types.generated.Node.Builder clearLongitude() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public Node build() {
      try {
        Node record = new Node();
        record.common = fieldSetFlags()[0] ? this.common : (mil.nga.giat.osm.types.generated.Primitive) defaultValue(fields()[0]);
        record.latitude = fieldSetFlags()[1] ? this.latitude : (java.lang.Double) defaultValue(fields()[1]);
        record.longitude = fieldSetFlags()[2] ? this.longitude : (java.lang.Double) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
