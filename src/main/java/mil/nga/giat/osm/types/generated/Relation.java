/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package mil.nga.giat.osm.types.generated;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Relation extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Relation\",\"namespace\":\"mil.nga.giat.osm.types.generated\",\"fields\":[{\"name\":\"common\",\"type\":{\"type\":\"record\",\"name\":\"Primitive\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"version\",\"type\":\"long\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"user_id\",\"type\":[\"null\",\"long\"]},{\"name\":\"user_name\",\"type\":[\"null\",\"string\"]},{\"name\":\"changeset_id\",\"type\":\"long\"},{\"name\":\"visible\",\"type\":\"boolean\",\"default\":\"true\"},{\"name\":\"tags\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}]}},{\"name\":\"members\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"RelationMember\",\"fields\":[{\"name\":\"role\",\"type\":[\"null\",\"string\"]},{\"name\":\"member\",\"type\":\"long\"},{\"name\":\"member_type\",\"type\":{\"type\":\"enum\",\"name\":\"MemberType\",\"symbols\":[\"NODE\",\"WAY\",\"RELATION\"]}}]}}]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public mil.nga.giat.osm.types.generated.Primitive common;
  @Deprecated public java.util.List<mil.nga.giat.osm.types.generated.RelationMember> members;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Relation() {}

  /**
   * All-args constructor.
   */
  public Relation(mil.nga.giat.osm.types.generated.Primitive common, java.util.List<mil.nga.giat.osm.types.generated.RelationMember> members) {
    this.common = common;
    this.members = members;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return common;
    case 1: return members;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: common = (mil.nga.giat.osm.types.generated.Primitive)value$; break;
    case 1: members = (java.util.List<mil.nga.giat.osm.types.generated.RelationMember>)value$; break;
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
   * Gets the value of the 'members' field.
   */
  public java.util.List<mil.nga.giat.osm.types.generated.RelationMember> getMembers() {
    return members;
  }

  /**
   * Sets the value of the 'members' field.
   * @param value the value to set.
   */
  public void setMembers(java.util.List<mil.nga.giat.osm.types.generated.RelationMember> value) {
    this.members = value;
  }

  /** Creates a new Relation RecordBuilder */
  public static mil.nga.giat.osm.types.generated.Relation.Builder newBuilder() {
    return new mil.nga.giat.osm.types.generated.Relation.Builder();
  }
  
  /** Creates a new Relation RecordBuilder by copying an existing Builder */
  public static mil.nga.giat.osm.types.generated.Relation.Builder newBuilder(mil.nga.giat.osm.types.generated.Relation.Builder other) {
    return new mil.nga.giat.osm.types.generated.Relation.Builder(other);
  }
  
  /** Creates a new Relation RecordBuilder by copying an existing Relation instance */
  public static mil.nga.giat.osm.types.generated.Relation.Builder newBuilder(mil.nga.giat.osm.types.generated.Relation other) {
    return new mil.nga.giat.osm.types.generated.Relation.Builder(other);
  }
  
  /**
   * RecordBuilder for Relation instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Relation>
    implements org.apache.avro.data.RecordBuilder<Relation> {

    private mil.nga.giat.osm.types.generated.Primitive common;
    private java.util.List<mil.nga.giat.osm.types.generated.RelationMember> members;

    /** Creates a new Builder */
    private Builder() {
      super(mil.nga.giat.osm.types.generated.Relation.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(mil.nga.giat.osm.types.generated.Relation.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.common)) {
        this.common = data().deepCopy(fields()[0].schema(), other.common);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.members)) {
        this.members = data().deepCopy(fields()[1].schema(), other.members);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Relation instance */
    private Builder(mil.nga.giat.osm.types.generated.Relation other) {
            super(mil.nga.giat.osm.types.generated.Relation.SCHEMA$);
      if (isValidValue(fields()[0], other.common)) {
        this.common = data().deepCopy(fields()[0].schema(), other.common);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.members)) {
        this.members = data().deepCopy(fields()[1].schema(), other.members);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Primitive getCommon() {
      return common;
    }
    
    /** Sets the value of the 'common' field */
    public mil.nga.giat.osm.types.generated.Relation.Builder setCommon(mil.nga.giat.osm.types.generated.Primitive value) {
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
    public mil.nga.giat.osm.types.generated.Relation.Builder clearCommon() {
      common = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'members' field */
    public java.util.List<mil.nga.giat.osm.types.generated.RelationMember> getMembers() {
      return members;
    }
    
    /** Sets the value of the 'members' field */
    public mil.nga.giat.osm.types.generated.Relation.Builder setMembers(java.util.List<mil.nga.giat.osm.types.generated.RelationMember> value) {
      validate(fields()[1], value);
      this.members = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'members' field has been set */
    public boolean hasMembers() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'members' field */
    public mil.nga.giat.osm.types.generated.Relation.Builder clearMembers() {
      members = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public Relation build() {
      try {
        Relation record = new Relation();
        record.common = fieldSetFlags()[0] ? this.common : (mil.nga.giat.osm.types.generated.Primitive) defaultValue(fields()[0]);
        record.members = fieldSetFlags()[1] ? this.members : (java.util.List<mil.nga.giat.osm.types.generated.RelationMember>) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}