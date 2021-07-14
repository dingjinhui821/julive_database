// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Fri Aug 28 11:29:51 CST 2020
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
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        id = (Long)value;
      }
    });
    setters.put("order_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        order_id = (Long)value;
      }
    });
    setters.put("employee_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        employee_id = (Long)value;
      }
    });
    setters.put("employee_manager_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        employee_manager_id = (Long)value;
      }
    });
    setters.put("new_mobile", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        new_mobile = (String)value;
      }
    });
    setters.put("old_mobile", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        old_mobile = (String)value;
      }
    });
    setters.put("new_more_mobile", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        new_more_mobile = (String)value;
      }
    });
    setters.put("old_more_mobile", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        old_more_mobile = (String)value;
      }
    });
    setters.put("audit_status", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        audit_status = (Integer)value;
      }
    });
    setters.put("auditor", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        auditor = (Long)value;
      }
    });
    setters.put("audit_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        audit_time = (Long)value;
      }
    });
    setters.put("operate_type", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        operate_type = (Integer)value;
      }
    });
    setters.put("create_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        create_time = (Long)value;
      }
    });
    setters.put("update_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        update_time = (Long)value;
      }
    });
    setters.put("creator", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        creator = (Long)value;
      }
    });
    setters.put("updator", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        updator = (Long)value;
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
  private Long id;
  public Long get_id() {
    return id;
  }
  public void set_id(Long id) {
    this.id = id;
  }
  public QueryResult with_id(Long id) {
    this.id = id;
    return this;
  }
  private Long order_id;
  public Long get_order_id() {
    return order_id;
  }
  public void set_order_id(Long order_id) {
    this.order_id = order_id;
  }
  public QueryResult with_order_id(Long order_id) {
    this.order_id = order_id;
    return this;
  }
  private Long employee_id;
  public Long get_employee_id() {
    return employee_id;
  }
  public void set_employee_id(Long employee_id) {
    this.employee_id = employee_id;
  }
  public QueryResult with_employee_id(Long employee_id) {
    this.employee_id = employee_id;
    return this;
  }
  private Long employee_manager_id;
  public Long get_employee_manager_id() {
    return employee_manager_id;
  }
  public void set_employee_manager_id(Long employee_manager_id) {
    this.employee_manager_id = employee_manager_id;
  }
  public QueryResult with_employee_manager_id(Long employee_manager_id) {
    this.employee_manager_id = employee_manager_id;
    return this;
  }
  private String new_mobile;
  public String get_new_mobile() {
    return new_mobile;
  }
  public void set_new_mobile(String new_mobile) {
    this.new_mobile = new_mobile;
  }
  public QueryResult with_new_mobile(String new_mobile) {
    this.new_mobile = new_mobile;
    return this;
  }
  private String old_mobile;
  public String get_old_mobile() {
    return old_mobile;
  }
  public void set_old_mobile(String old_mobile) {
    this.old_mobile = old_mobile;
  }
  public QueryResult with_old_mobile(String old_mobile) {
    this.old_mobile = old_mobile;
    return this;
  }
  private String new_more_mobile;
  public String get_new_more_mobile() {
    return new_more_mobile;
  }
  public void set_new_more_mobile(String new_more_mobile) {
    this.new_more_mobile = new_more_mobile;
  }
  public QueryResult with_new_more_mobile(String new_more_mobile) {
    this.new_more_mobile = new_more_mobile;
    return this;
  }
  private String old_more_mobile;
  public String get_old_more_mobile() {
    return old_more_mobile;
  }
  public void set_old_more_mobile(String old_more_mobile) {
    this.old_more_mobile = old_more_mobile;
  }
  public QueryResult with_old_more_mobile(String old_more_mobile) {
    this.old_more_mobile = old_more_mobile;
    return this;
  }
  private Integer audit_status;
  public Integer get_audit_status() {
    return audit_status;
  }
  public void set_audit_status(Integer audit_status) {
    this.audit_status = audit_status;
  }
  public QueryResult with_audit_status(Integer audit_status) {
    this.audit_status = audit_status;
    return this;
  }
  private Long auditor;
  public Long get_auditor() {
    return auditor;
  }
  public void set_auditor(Long auditor) {
    this.auditor = auditor;
  }
  public QueryResult with_auditor(Long auditor) {
    this.auditor = auditor;
    return this;
  }
  private Long audit_time;
  public Long get_audit_time() {
    return audit_time;
  }
  public void set_audit_time(Long audit_time) {
    this.audit_time = audit_time;
  }
  public QueryResult with_audit_time(Long audit_time) {
    this.audit_time = audit_time;
    return this;
  }
  private Integer operate_type;
  public Integer get_operate_type() {
    return operate_type;
  }
  public void set_operate_type(Integer operate_type) {
    this.operate_type = operate_type;
  }
  public QueryResult with_operate_type(Integer operate_type) {
    this.operate_type = operate_type;
    return this;
  }
  private Long create_time;
  public Long get_create_time() {
    return create_time;
  }
  public void set_create_time(Long create_time) {
    this.create_time = create_time;
  }
  public QueryResult with_create_time(Long create_time) {
    this.create_time = create_time;
    return this;
  }
  private Long update_time;
  public Long get_update_time() {
    return update_time;
  }
  public void set_update_time(Long update_time) {
    this.update_time = update_time;
  }
  public QueryResult with_update_time(Long update_time) {
    this.update_time = update_time;
    return this;
  }
  private Long creator;
  public Long get_creator() {
    return creator;
  }
  public void set_creator(Long creator) {
    this.creator = creator;
  }
  public QueryResult with_creator(Long creator) {
    this.creator = creator;
    return this;
  }
  private Long updator;
  public Long get_updator() {
    return updator;
  }
  public void set_updator(Long updator) {
    this.updator = updator;
  }
  public QueryResult with_updator(Long updator) {
    this.updator = updator;
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
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.order_id == null ? that.order_id == null : this.order_id.equals(that.order_id));
    equal = equal && (this.employee_id == null ? that.employee_id == null : this.employee_id.equals(that.employee_id));
    equal = equal && (this.employee_manager_id == null ? that.employee_manager_id == null : this.employee_manager_id.equals(that.employee_manager_id));
    equal = equal && (this.new_mobile == null ? that.new_mobile == null : this.new_mobile.equals(that.new_mobile));
    equal = equal && (this.old_mobile == null ? that.old_mobile == null : this.old_mobile.equals(that.old_mobile));
    equal = equal && (this.new_more_mobile == null ? that.new_more_mobile == null : this.new_more_mobile.equals(that.new_more_mobile));
    equal = equal && (this.old_more_mobile == null ? that.old_more_mobile == null : this.old_more_mobile.equals(that.old_more_mobile));
    equal = equal && (this.audit_status == null ? that.audit_status == null : this.audit_status.equals(that.audit_status));
    equal = equal && (this.auditor == null ? that.auditor == null : this.auditor.equals(that.auditor));
    equal = equal && (this.audit_time == null ? that.audit_time == null : this.audit_time.equals(that.audit_time));
    equal = equal && (this.operate_type == null ? that.operate_type == null : this.operate_type.equals(that.operate_type));
    equal = equal && (this.create_time == null ? that.create_time == null : this.create_time.equals(that.create_time));
    equal = equal && (this.update_time == null ? that.update_time == null : this.update_time.equals(that.update_time));
    equal = equal && (this.creator == null ? that.creator == null : this.creator.equals(that.creator));
    equal = equal && (this.updator == null ? that.updator == null : this.updator.equals(that.updator));
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
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.order_id == null ? that.order_id == null : this.order_id.equals(that.order_id));
    equal = equal && (this.employee_id == null ? that.employee_id == null : this.employee_id.equals(that.employee_id));
    equal = equal && (this.employee_manager_id == null ? that.employee_manager_id == null : this.employee_manager_id.equals(that.employee_manager_id));
    equal = equal && (this.new_mobile == null ? that.new_mobile == null : this.new_mobile.equals(that.new_mobile));
    equal = equal && (this.old_mobile == null ? that.old_mobile == null : this.old_mobile.equals(that.old_mobile));
    equal = equal && (this.new_more_mobile == null ? that.new_more_mobile == null : this.new_more_mobile.equals(that.new_more_mobile));
    equal = equal && (this.old_more_mobile == null ? that.old_more_mobile == null : this.old_more_mobile.equals(that.old_more_mobile));
    equal = equal && (this.audit_status == null ? that.audit_status == null : this.audit_status.equals(that.audit_status));
    equal = equal && (this.auditor == null ? that.auditor == null : this.auditor.equals(that.auditor));
    equal = equal && (this.audit_time == null ? that.audit_time == null : this.audit_time.equals(that.audit_time));
    equal = equal && (this.operate_type == null ? that.operate_type == null : this.operate_type.equals(that.operate_type));
    equal = equal && (this.create_time == null ? that.create_time == null : this.create_time.equals(that.create_time));
    equal = equal && (this.update_time == null ? that.update_time == null : this.update_time.equals(that.update_time));
    equal = equal && (this.creator == null ? that.creator == null : this.creator.equals(that.creator));
    equal = equal && (this.updator == null ? that.updator == null : this.updator.equals(that.updator));
    equal = equal && (this.etl_time == null ? that.etl_time == null : this.etl_time.equals(that.etl_time));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.order_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.employee_id = JdbcWritableBridge.readLong(3, __dbResults);
    this.employee_manager_id = JdbcWritableBridge.readLong(4, __dbResults);
    this.new_mobile = JdbcWritableBridge.readString(5, __dbResults);
    this.old_mobile = JdbcWritableBridge.readString(6, __dbResults);
    this.new_more_mobile = JdbcWritableBridge.readString(7, __dbResults);
    this.old_more_mobile = JdbcWritableBridge.readString(8, __dbResults);
    this.audit_status = JdbcWritableBridge.readInteger(9, __dbResults);
    this.auditor = JdbcWritableBridge.readLong(10, __dbResults);
    this.audit_time = JdbcWritableBridge.readLong(11, __dbResults);
    this.operate_type = JdbcWritableBridge.readInteger(12, __dbResults);
    this.create_time = JdbcWritableBridge.readLong(13, __dbResults);
    this.update_time = JdbcWritableBridge.readLong(14, __dbResults);
    this.creator = JdbcWritableBridge.readLong(15, __dbResults);
    this.updator = JdbcWritableBridge.readLong(16, __dbResults);
    this.etl_time = JdbcWritableBridge.readTimestamp(17, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.order_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.employee_id = JdbcWritableBridge.readLong(3, __dbResults);
    this.employee_manager_id = JdbcWritableBridge.readLong(4, __dbResults);
    this.new_mobile = JdbcWritableBridge.readString(5, __dbResults);
    this.old_mobile = JdbcWritableBridge.readString(6, __dbResults);
    this.new_more_mobile = JdbcWritableBridge.readString(7, __dbResults);
    this.old_more_mobile = JdbcWritableBridge.readString(8, __dbResults);
    this.audit_status = JdbcWritableBridge.readInteger(9, __dbResults);
    this.auditor = JdbcWritableBridge.readLong(10, __dbResults);
    this.audit_time = JdbcWritableBridge.readLong(11, __dbResults);
    this.operate_type = JdbcWritableBridge.readInteger(12, __dbResults);
    this.create_time = JdbcWritableBridge.readLong(13, __dbResults);
    this.update_time = JdbcWritableBridge.readLong(14, __dbResults);
    this.creator = JdbcWritableBridge.readLong(15, __dbResults);
    this.updator = JdbcWritableBridge.readLong(16, __dbResults);
    this.etl_time = JdbcWritableBridge.readTimestamp(17, __dbResults);
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
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(order_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(employee_id, 3 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(employee_manager_id, 4 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(new_mobile, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(old_mobile, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(new_more_mobile, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(old_more_mobile, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(audit_status, 9 + __off, -6, __dbStmt);
    JdbcWritableBridge.writeLong(auditor, 10 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(audit_time, 11 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(operate_type, 12 + __off, -6, __dbStmt);
    JdbcWritableBridge.writeLong(create_time, 13 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(update_time, 14 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(creator, 15 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(updator, 16 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeTimestamp(etl_time, 17 + __off, 93, __dbStmt);
    return 17;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(order_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(employee_id, 3 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(employee_manager_id, 4 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(new_mobile, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(old_mobile, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(new_more_mobile, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(old_more_mobile, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(audit_status, 9 + __off, -6, __dbStmt);
    JdbcWritableBridge.writeLong(auditor, 10 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(audit_time, 11 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(operate_type, 12 + __off, -6, __dbStmt);
    JdbcWritableBridge.writeLong(create_time, 13 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(update_time, 14 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(creator, 15 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(updator, 16 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeTimestamp(etl_time, 17 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.order_id = null;
    } else {
    this.order_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.employee_id = null;
    } else {
    this.employee_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.employee_manager_id = null;
    } else {
    this.employee_manager_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.new_mobile = null;
    } else {
    this.new_mobile = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.old_mobile = null;
    } else {
    this.old_mobile = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.new_more_mobile = null;
    } else {
    this.new_more_mobile = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.old_more_mobile = null;
    } else {
    this.old_more_mobile = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.audit_status = null;
    } else {
    this.audit_status = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.auditor = null;
    } else {
    this.auditor = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.audit_time = null;
    } else {
    this.audit_time = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.operate_type = null;
    } else {
    this.operate_type = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.create_time = null;
    } else {
    this.create_time = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.update_time = null;
    } else {
    this.update_time = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.creator = null;
    } else {
    this.creator = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.updator = null;
    } else {
    this.updator = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.etl_time = null;
    } else {
    this.etl_time = new Timestamp(__dataIn.readLong());
    this.etl_time.setNanos(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.order_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.order_id);
    }
    if (null == this.employee_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.employee_id);
    }
    if (null == this.employee_manager_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.employee_manager_id);
    }
    if (null == this.new_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, new_mobile);
    }
    if (null == this.old_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, old_mobile);
    }
    if (null == this.new_more_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, new_more_mobile);
    }
    if (null == this.old_more_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, old_more_mobile);
    }
    if (null == this.audit_status) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.audit_status);
    }
    if (null == this.auditor) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.auditor);
    }
    if (null == this.audit_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.audit_time);
    }
    if (null == this.operate_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.operate_type);
    }
    if (null == this.create_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.create_time);
    }
    if (null == this.update_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.update_time);
    }
    if (null == this.creator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.creator);
    }
    if (null == this.updator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.updator);
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
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.order_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.order_id);
    }
    if (null == this.employee_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.employee_id);
    }
    if (null == this.employee_manager_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.employee_manager_id);
    }
    if (null == this.new_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, new_mobile);
    }
    if (null == this.old_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, old_mobile);
    }
    if (null == this.new_more_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, new_more_mobile);
    }
    if (null == this.old_more_mobile) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, old_more_mobile);
    }
    if (null == this.audit_status) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.audit_status);
    }
    if (null == this.auditor) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.auditor);
    }
    if (null == this.audit_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.audit_time);
    }
    if (null == this.operate_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.operate_type);
    }
    if (null == this.create_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.create_time);
    }
    if (null == this.update_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.update_time);
    }
    if (null == this.creator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.creator);
    }
    if (null == this.updator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.updator);
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
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_id==null?"null":"" + order_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(employee_id==null?"null":"" + employee_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(employee_manager_id==null?"null":"" + employee_manager_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(new_mobile==null?"null":new_mobile, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(old_mobile==null?"null":old_mobile, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(new_more_mobile==null?"null":new_more_mobile, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(old_more_mobile==null?"null":old_more_mobile, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(audit_status==null?"null":"" + audit_status, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(auditor==null?"null":"" + auditor, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(audit_time==null?"null":"" + audit_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(operate_type==null?"null":"" + operate_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(create_time==null?"null":"" + create_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(update_time==null?"null":"" + update_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(creator==null?"null":"" + creator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(updator==null?"null":"" + updator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(etl_time==null?"null":"" + etl_time, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_id==null?"null":"" + order_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(employee_id==null?"null":"" + employee_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(employee_manager_id==null?"null":"" + employee_manager_id, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(new_mobile==null?"null":new_mobile, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(old_mobile==null?"null":old_mobile, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(new_more_mobile==null?"null":new_more_mobile, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(old_more_mobile==null?"null":old_more_mobile, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(audit_status==null?"null":"" + audit_status, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(auditor==null?"null":"" + auditor, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(audit_time==null?"null":"" + audit_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(operate_type==null?"null":"" + operate_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(create_time==null?"null":"" + create_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(update_time==null?"null":"" + update_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(creator==null?"null":"" + creator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(updator==null?"null":"" + updator, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.order_id = null; } else {
      this.order_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.employee_id = null; } else {
      this.employee_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.employee_manager_id = null; } else {
      this.employee_manager_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.new_mobile = null; } else {
      this.new_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.old_mobile = null; } else {
      this.old_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.new_more_mobile = null; } else {
      this.new_more_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.old_more_mobile = null; } else {
      this.old_more_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.audit_status = null; } else {
      this.audit_status = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.auditor = null; } else {
      this.auditor = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.audit_time = null; } else {
      this.audit_time = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.operate_type = null; } else {
      this.operate_type = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.create_time = null; } else {
      this.create_time = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.update_time = null; } else {
      this.update_time = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.creator = null; } else {
      this.creator = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.updator = null; } else {
      this.updator = Long.valueOf(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.order_id = null; } else {
      this.order_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.employee_id = null; } else {
      this.employee_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.employee_manager_id = null; } else {
      this.employee_manager_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.new_mobile = null; } else {
      this.new_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.old_mobile = null; } else {
      this.old_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.new_more_mobile = null; } else {
      this.new_more_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.old_more_mobile = null; } else {
      this.old_more_mobile = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.audit_status = null; } else {
      this.audit_status = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.auditor = null; } else {
      this.auditor = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.audit_time = null; } else {
      this.audit_time = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.operate_type = null; } else {
      this.operate_type = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.create_time = null; } else {
      this.create_time = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.update_time = null; } else {
      this.update_time = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.creator = null; } else {
      this.creator = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.updator = null; } else {
      this.updator = Long.valueOf(__cur_str);
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
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("order_id", this.order_id);
    __sqoop$field_map.put("employee_id", this.employee_id);
    __sqoop$field_map.put("employee_manager_id", this.employee_manager_id);
    __sqoop$field_map.put("new_mobile", this.new_mobile);
    __sqoop$field_map.put("old_mobile", this.old_mobile);
    __sqoop$field_map.put("new_more_mobile", this.new_more_mobile);
    __sqoop$field_map.put("old_more_mobile", this.old_more_mobile);
    __sqoop$field_map.put("audit_status", this.audit_status);
    __sqoop$field_map.put("auditor", this.auditor);
    __sqoop$field_map.put("audit_time", this.audit_time);
    __sqoop$field_map.put("operate_type", this.operate_type);
    __sqoop$field_map.put("create_time", this.create_time);
    __sqoop$field_map.put("update_time", this.update_time);
    __sqoop$field_map.put("creator", this.creator);
    __sqoop$field_map.put("updator", this.updator);
    __sqoop$field_map.put("etl_time", this.etl_time);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("order_id", this.order_id);
    __sqoop$field_map.put("employee_id", this.employee_id);
    __sqoop$field_map.put("employee_manager_id", this.employee_manager_id);
    __sqoop$field_map.put("new_mobile", this.new_mobile);
    __sqoop$field_map.put("old_mobile", this.old_mobile);
    __sqoop$field_map.put("new_more_mobile", this.new_more_mobile);
    __sqoop$field_map.put("old_more_mobile", this.old_more_mobile);
    __sqoop$field_map.put("audit_status", this.audit_status);
    __sqoop$field_map.put("auditor", this.auditor);
    __sqoop$field_map.put("audit_time", this.audit_time);
    __sqoop$field_map.put("operate_type", this.operate_type);
    __sqoop$field_map.put("create_time", this.create_time);
    __sqoop$field_map.put("update_time", this.update_time);
    __sqoop$field_map.put("creator", this.creator);
    __sqoop$field_map.put("updator", this.updator);
    __sqoop$field_map.put("etl_time", this.etl_time);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
