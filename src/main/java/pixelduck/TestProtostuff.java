package pixelduck;

import java.io.FileOutputStream;
import java.io.IOException;
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
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class WrapperMapObjectByString {
    private Map<String, Object> mapObjectByString;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class WrapperMapBByString {
    private Map<String, B> mapBByString;
  }

  static {
    RuntimeSchema.register(A.class, new ASchema());
    RuntimeSchema.register(B.class, new BSchema());
  }

  public static void main(String[] args) throws IOException {

    A a = new A();
    a.name = "A-name";
    a.mapOfB.put("first-key", new B("first"));
    a.mapOfB.put("second-key", new B("second"));
    a.mapOfObject.put("third-key", new B("third"));
    a.mapOfObject.put("fourth-key", new B("fourth"));

    try (OutputStream os = new FileOutputStream("/tmp/demo.serps")) {
      LinkedBuffer linkedBuffer = LinkedBuffer.allocate();
      ProtobufIOUtil.writeTo(os, a, RuntimeSchema.getSchema(A.class), linkedBuffer);
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
    private final static int INDEX_ID = 2;
    private final static int INDEX_VERSION = 3;

    @Override
    public String getFieldName(int i) {
      switch(i) {
      case INDEX_NAME: return "name";
      case INDEX_ID: return "id";
      case INDEX_VERSION: return "version";
      default: return null;
      }
    }

    @Override
    public int getFieldNumber(String s) {
      switch(s) {
      case "name": return INDEX_NAME;
      case "id": return INDEX_ID;
      case "version": return INDEX_VERSION;
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
          b.name = input.readString();
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
    }
  }
}
