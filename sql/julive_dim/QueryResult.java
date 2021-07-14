// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Thu Oct 24 11:18:15 CST 2019
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("skey", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        skey = (Integer)value;
      }
    });
    setters.put("city_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        city_id = (Long)value;
      }
    });
    setters.put("city_name", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        city_name = (String)value;
      }
    });
    setters.put("city_seq", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        city_seq = (String)value;
      }
    });
    setters.put("region", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        region = (String)value;
      }
    });
    setters.put("etl_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        etl_time = (java.sql.Timestamp)value;
      }
    });
  }
  public QueryResult() {
    init0();
  }
  private Integer skey;
  public Integer get_skey() {
    return skey;
  }
  public void set_skey(Integer skey) {
    this.skey = skey;
  }
  public QueryResult with_skey(Integer skey) {
    this.skey = skey;
    return this;
  }
  private Long city_id;
  public Long get_city_id() {
    return city_id;
  }
  public void set_city_id(Long city_id) {
    this.city_id = city_id;
  }
  public QueryResult with_city_id(Long city_id) {
    this.city_id = city_id;
    return this;
  }
  private String city_name;
  public String get_city_name() {
    return city_name;
  }
  public void set_city_name(String city_name) {
    this.city_name = city_name;
  }
  public QueryResult with_city_name(String city_name) {
    this.city_name = city_name;
    return this;
  }
  private String city_seq;
  public String get_city_seq() {
    return city_seq;
  }
  public void set_city_seq(String city_seq) {
    this.city_seq = city_seq;
  }
  public QueryResult with_city_seq(String city_seq) {
    this.city_seq = city_seq;
    return this;
  }
  private String region;
  public String get_region() {
    return region;
  }
  public void set_region(String region) {
    this.region = region;
  }
  public QueryResult with_region(String region) {
    this.region = region;
    return this;
  }
  private java.sql.Timestamp etl_time;
  public java.sql.Timestamp get_etl_time() {
    return etl_time;
  }
  public void set_etl_time(java.sql.Timestamp etl_time) {
    this.etl_time = etl_time;
  }
  public QueryResult with_etl_time(java.sql.Timestamp etl_time) {
    this.etl_time = etl_time;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.skey == null ? that.skey == null : this.skey.equals(that.skey));
    equal = equal && (this.city_id == null ? that.city_id == null : this.city_id.equals(that.city_id));
    equal = equal && (this.city_name == null ? that.city_name == null : this.city_name.equals(that.city_name));
    equal = equal && (this.city_seq == null ? that.city_seq == null : this.city_seq.equals(that.city_seq));
    equal = equal && (this.region == null ? that.region == null : this.region.equals(that.region));
    equal = equal && (this.etl_time == null ? that.etl_time == null : this.etl_time.equals(that.etl_time));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.skey == null ? that.skey == null : this.skey.equals(that.skey));
    equal = equal && (this.city_id == null ? that.city_id == null : this.city_id.equals(that.city_id));
    equal = equal && (this.city_name == null ? that.city_name == null : this.city_name.equals(that.city_name));
    equal = equal && (this.city_seq == null ? that.city_seq == null : this.city_seq.equals(that.city_seq));
    equal = equal && (this.region == null ? that.region == null : this.region.equals(that.region));
    equal = equal && (this.etl_time == null ? that.etl_time == null : this.etl_time.equals(that.etl_time));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.skey = JdbcWritableBridge.readInteger(1, __dbResults);
    this.city_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.city_name = JdbcWritableBridge.readString(3, __dbResults);
    this.city_seq = JdbcWritableBridge.readString(4, __dbResults);
    this.region = JdbcWritableBridge.readString(5, __dbResults);
    this.etl_time = JdbcWritableBridge.readTimestamp(6, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.skey = JdbcWritableBridge.readInteger(1, __dbResults);
    this.city_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.city_name = JdbcWritableBridge.readString(3, __dbResults);
    this.city_seq = JdbcWritableBridge.readString(4, __dbResults);
    this.region = JdbcWritableBridge.readString(5, __dbResults);
    this.etl_time = JdbcWritableBridge.readTimestamp(6, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(skey, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeLong(city_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(city_name, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city_seq, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(region, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(etl_time, 6 + __off, 93, __dbStmt);
    return 6;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(skey, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeLong(city_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(city_name, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city_seq, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(region, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(etl_time, 6 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.skey = null;
    } else {
    this.skey = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.city_id = null;
    } else {
    this.city_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.city_name = null;
    } else {
    this.city_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city_seq = null;
    } else {
    this.city_seq = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.region = null;
    } else {
    this.region = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.etl_time = null;
    } else {
    this.etl_time = new Timestamp(__dataIn.readLong());
    this.etl_time.setNanos(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.skey) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.skey);
    }
    if (null == this.city_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.city_id);
    }
    if (null == this.city_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city_name);
    }
    if (null == this.city_seq) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city_seq);
    }
    if (null == this.region) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, region);
    }
    if (null == this.etl_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.etl_time.getTime());
    __dataOut.writeInt(this.etl_time.getNanos());
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.skey) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.skey);
    }
    if (null == this.city_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.city_id);
    }
    if (null == this.city_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city_name);
    }
    if (null == this.city_seq) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city_seq);
    }
    if (null == this.region) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, region);
    }
    if (null == this.etl_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.etl_time.getTime());
    __dataOut.writeInt(this.etl_time.getNanos());
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(skey==null?"null":"" + skey, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city_id==null?"null":"" + city_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(city_name==null?"null":city_name, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(city_seq==null?"null":city_seq, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(region==null?"null":region, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(etl_time==null?"null":"" + etl_time, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(skey==null?"null":"" + skey, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city_id==null?"null":"" + city_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(city_name==null?"null":city_name, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(city_seq==null?"null":city_seq, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(region==null?"null":region, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(etl_time==null?"null":"" + etl_time, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.skey = null; } else {
      this.skey = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.city_id = null; } else {
      this.city_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city_name = null; } else {
      this.city_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city_seq = null; } else {
      this.city_seq = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.region = null; } else {
      this.region = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.etl_time = null; } else {
      this.etl_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.skey = null; } else {
      this.skey = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.city_id = null; } else {
      this.city_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city_name = null; } else {
      this.city_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city_seq = null; } else {
      this.city_seq = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.region = null; } else {
      this.region = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.etl_time = null; } else {
      this.etl_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.etl_time = (o.etl_time != null) ? (java.sql.Timestamp) o.etl_time.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.etl_time = (o.etl_time != null) ? (java.sql.Timestamp) o.etl_time.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("skey", this.skey);
    __sqoop$field_map.put("city_id", this.city_id);
    __sqoop$field_map.put("city_name", this.city_name);
    __sqoop$field_map.put("city_seq", this.city_seq);
    __sqoop$field_map.put("region", this.region);
    __sqoop$field_map.put("etl_time", this.etl_time);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("skey", this.skey);
    __sqoop$field_map.put("city_id", this.city_id);
    __sqoop$field_map.put("city_name", this.city_name);
    __sqoop$field_map.put("city_seq", this.city_seq);
    __sqoop$field_map.put("region", this.region);
    __sqoop$field_map.put("etl_time", this.etl_time);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
