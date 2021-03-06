package pixelduck;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.Output;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.UninitializedMessageException;
import io.protostuff.runtime.RuntimeSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Test protostuff.
 */
public class TestProtostuff {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class A {
    private String name;
    private Map<String, B> mapOfB = new HashMap<>();
    private Map<String, Object> mapOfObject = new HashMap<>();
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class B {
    private String name;
//    private int version;
    private String author;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class WrapperMapObjectByString {
    private Map<String, Object> mapObjectByString;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class WrapperMapBByString {
    private Map<String, B> mapBByString;
  }

  public static void main(String[] args) throws IOException {
    RuntimeSchema.register(A.class, new ASchema());
    RuntimeSchema.register(B.class, new BSchema());
    A a = new A();
    a.name = "A-name";
//    a.mapOfB.put("first-key", new B("first"));
//    a.mapOfB.put("second-key", new B("second"));
//    a.mapOfObject.put("third-key", new B("third"));
//    a.mapOfObject.put("fourth-key", new B("fourth"));
//    a.mapOfB.put("first-key", new B("first", 1, "oma"));
//    a.mapOfB.put("second-key", new B("second", 1, "oma"));
//    a.mapOfObject.put("third-key", new B("third", 1, "oma"));
//    a.mapOfObject.put("fourth-key", new B("fourth", 1, "oma"));
    a.mapOfB.put("first-key", new B("first", "oma"));
    a.mapOfB.put("second-key", new B("second", "oma"));
    a.mapOfObject.put("third-key", new B("third", "oma"));
    a.mapOfObject.put("fourth-key", new B("fourth", "oma"));

    write("demo", 3, a);
    read("demo", 1);
    read("demo", 2);
    read("demo", 3);
  }

  public static void write(String key, int version, A a) throws IOException {
    String dir = System.getProperty("user.home") + "/tmp/TestProtostuff/src/main/resources/";
    try (OutputStream os = new FileOutputStream(dir + key + "_v" + version + ".serps")) {
      LinkedBuffer linkedBuffer = LinkedBuffer.allocate();
      ProtobufIOUtil.writeTo(os, a, new ASchema(), linkedBuffer);
    }
  }

  public static void read(String key, int version) throws IOException {
    String dir = System.getProperty("user.home") + "/tmp/TestProtostuff/src/main/resources/";
    try (InputStream io = new FileInputStream(dir + key + "_v" + version + ".serps")) {
      Schema<A> schema  = new ASchema();
      A ret = schema.newMessage();
      ProtobufIOUtil.mergeFrom(io, ret, schema);
      System.out.println("Read version " + version + " => " + ret);
    }
  }

  static class ASchema implements Schema<A> {
    private final static int INDEX_NAME = 1;
    private final static int INDEX_MAP_OF_B = 2;
    private final static int INDEX_MAP_OF_OBJECT = 3;

    @Override
    public String getFieldName(int i) {
      switch(i) {
      case INDEX_NAME: return "name";
      case INDEX_MAP_OF_B: return "mapOfB";
      case INDEX_MAP_OF_OBJECT: return "mapOfObject";
      default: return null;
      }
    }

    @Override
    public int getFieldNumber(String s) {
      switch(s) {
      case "name": return INDEX_NAME;
      case "mapOfB": return INDEX_MAP_OF_B;
      case "mapOfObject": return INDEX_MAP_OF_OBJECT;
      default: return 0;
      }
    }

    @Override
    public boolean isInitialized(A a) {
      return a.getName() != null;
    }

    @Override
    public A newMessage() {
      return new A();
    }

    @Override
    public String messageName() {
      return "MyClassA";
    }

    @Override
    public String messageFullName() {
      return "MyClassA";
    }

    @Override
    public Class<? super A> typeClass() {
      return A.class;
    }

    @Override
    public void mergeFrom(Input input, A a) throws IOException {
      while(true) {
        int number = input.readFieldNumber(this);
        switch(number) {
        case 0: // indicates end of serialization
          return;
        case INDEX_NAME:
          a.setName(input.readString());
          break;
        case INDEX_MAP_OF_B:
          a.mapOfB.putAll(input.mergeObject(null, RuntimeSchema.getSchema(WrapperMapBByString.class)).getMapBByString());
          break;
        case INDEX_MAP_OF_OBJECT:
          a.mapOfObject.putAll(input.mergeObject(null, RuntimeSchema.getSchema(WrapperMapObjectByString.class)).getMapObjectByString());
          break;
        default:
          input.handleUnknownField(number, this);
          break;
        }
      }
    }

    @Override
    public void writeTo(Output output, A a) throws IOException {
      if (a.getName() == null) {
        throw new UninitializedMessageException(a, this);
      }
      output.writeString(INDEX_NAME, a.getName(), false);
      output.writeObject(INDEX_MAP_OF_B, new WrapperMapBByString(a.mapOfB), RuntimeSchema.getSchema(WrapperMapBByString.class), false);
      output.writeObject(INDEX_MAP_OF_OBJECT, new WrapperMapObjectByString(a.mapOfObject), RuntimeSchema.getSchema(WrapperMapObjectByString.class), false);
    }
  }

  static class BSchema implements Schema<B> {
    private final static int INDEX_NAME = 1;
    private final static int INDEX_VERSION = 2;
    private final static int INDEX_AUTHOR = 3;

    @Override
    public String getFieldName(int i) {
      switch(i) {
      case INDEX_NAME: return "name";
      case INDEX_VERSION: return "version";
      case INDEX_AUTHOR: return "author";
      default: return null;
      }
    }

    @Override
    public int getFieldNumber(String s) {
      switch(s) {
      case "name": return INDEX_NAME;
      case "version": return INDEX_VERSION;
      case "author": return INDEX_AUTHOR;
      default: return 0;
      }
    }

    @Override
    public boolean isInitialized(B b) {
      return b.getName() != null;
    }

    @Override
    public B newMessage() {
      return new B();
    }

    @Override
    public String messageName() {
      return "MyClassB";
    }

    @Override
    public String messageFullName() {
      return "MyClassB";
    }

    @Override
    public Class<? super B> typeClass() {
      return B.class;
    }

    @Override
    public void mergeFrom(Input input, B b) throws IOException {
      while(true) {
        int number = input.readFieldNumber(this);
        switch(number) {
        case 0: // indicates end of serialization
          return;
        case INDEX_NAME:
          b.setName(input.readString());
          break;
//        case INDEX_VERSION:
//          b.version = input.readInt32();
//          break;
        case INDEX_AUTHOR:
          b.setAuthor(input.readString());
          break;
        default:
          input.handleUnknownField(number, this);
          break;
        }
      }
    }

    @Override
    public void writeTo(Output output, B b) throws IOException {
      if (b.getName() == null) {
        throw new UninitializedMessageException(b, this);
      }
      output.writeString(INDEX_NAME, b.getName(), false);
//      output.writeInt32(INDEX_VERSION, b.getVersion(), false);
      output.writeString(INDEX_AUTHOR, b.getAuthor(), false);
    }
  }
}
