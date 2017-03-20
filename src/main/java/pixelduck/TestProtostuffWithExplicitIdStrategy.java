package pixelduck;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import io.protostuff.CollectionSchema;
import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.MapSchema;
import io.protostuff.Output;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.UninitializedMessageException;
import io.protostuff.runtime.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import pixelduck.pojo.B;
import pixelduck.protostuff.BSer;

/**
 * Test protostuff with an ExplicitIdStrategy use to save metadata.
 */
public class TestProtostuffWithExplicitIdStrategy {

  public static class IdStrategyFactory implements IdStrategy.Factory {

    static int INSTANCE_COUNT = 0;

    ExplicitIdStrategy.Registry r = new ExplicitIdStrategy.Registry();

    public IdStrategyFactory()
    {
      ++INSTANCE_COUNT;
    }

    @Override
    public IdStrategy create()
    {
      return r.strategy;
    }

    @Override
    public void postCreate()
    {
      r.registerCollection(CollectionSchema.MessageFactories.ArrayList, 1)
          .registerCollection(CollectionSchema.MessageFactories.HashSet, 2)
          .registerCollection(CollectionSchema.MessageFactories.LinkedList, 3)
          .registerCollection(CollectionSchema.MessageFactories.TreeSet, 4);

      r.registerMap(MapSchema.MessageFactories.HashMap, 1)
          .registerMap(MapSchema.MessageFactories.LinkedHashMap, 2)
          .registerMap(MapSchema.MessageFactories.TreeMap, 3);

      r.registerPojo(A.class, 1)
          .registerPojo(BSer.B_SCHEMA, BSer.B_PIPE_SCHEMA, 2)
          .registerPojo(WrapperMapObjectByString.class, 3)
          .registerPojo(WrapperMapBByString.class, 4);
//      r = null;
    }
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


  public static void main(String[] args) throws IOException {
    System.setProperty("protostuff.runtime.id_strategy_factory",
        "pixelduck.TestProtostuffWithExplicitIdStrategy$IdStrategyFactory");
//    RuntimeSchema.register(A.class, new ASchema());
//    RuntimeSchema.register(B.class, new BSchema());
    A a = new A();
    a.setName("A-name");
//    a.getMapOfB().put("first-key", new B("first"));
//    a.getMapOfB().put("second-key", new B("second"));
//    a.getMapOfObject().put("third-key", new B("third"));
//    a.getMapOfObject().put("fourth-key", new B("fourth"));
    a.getMapOfB().put("first-key", new B("first", 1, "oma"));
    a.getMapOfB().put("second-key", new B("second", 1, "oma"));
    a.getMapOfObject().put("third-key", new B("third", 1, "oma"));
    a.getMapOfObject().put("fourth-key", new B("fourth", 1, "oma"));
//    a.getMapOfB().put("first-key", new B("first", "oma"));
//    a.getMapOfB().put("second-key", new B("second", "oma"));
//    a.getMapOfObject().put("third-key", new B("third", "oma"));
//    a.getMapOfObject().put("fourth-key", new B("fourth", "oma"));

    write("demo_expIdStrat", 5, a);
    // first we use pojo.B with a unique field name
    read("demo_expIdStrat", 1);
    // then we add two new fields
    read("demo_expIdStrat", 2);
    // now we use the same pojo in a different package with teh same field
    read("demo_expIdStrat", 3);
    // now we remove the field in the middle
    read("demo_expIdStrat", 4);
    // finally we get pack to the original class in pojo package but with the field which was removed
    read("demo_expIdStrat", 5);
  }

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
    public static class WrapperMapObjectByString {
      private Map<String, Object> mapObjectByString;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WrapperMapBByString {
      private Map<String, B> mapBByString;
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

}
