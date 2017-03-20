package pixelduck;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 * Created by olmartin on 3/17/17.
 */
public class AdvancedProtostuffTest {

  public static void main(String[] args) throws IOException {
    ASchema schema = new ASchema();

    // v1 - simple class with String
    //    A a = new A();
    //    a.setName("toto");
    // v5 - rename class
    //    com.expedia.gps.geo.service.feature.testprotostuff.A a = new com.expedia.gps.geo.service.feature.testprotostuff.A();
    // v6 - rename and move class
    //    com.expedia.gps.geo.service.feature.testprotostuff.A a = new com.expedia.gps.geo.service.feature.testprotostuff.A();

    // v2 - add a new Integer field
    //    a.version = 42; // v4 - remove field

    // v7 - go back to internal class A and add a new field which is a prototuff class
    A a = new A();
    a.setName("toto");
    a.setB(new B());
    a.getB().setName("maggie");
    //    a.getB().setId(1);

    // v3 - add a new field which is a list of String
    a.getListOfStrings().add("Marge");
    a.getListOfStrings().add("Homer");
    a.getListOfStrings().add("Bart");

    // v8 - add a new field in B but with a null value
    a.getB().setVersion(null);

    // v9 - remove field on B

    // v10 - move B class

    // v11 - go back to internal class B and add a new filed in A which is an hashmap[String, B]
    a.getBs().put("bla", new B("Bart", "v2"));
    a.getBs().put("boo", new B("Homer", "v3"));

    // v12 - move B class

    // v13 - go back to internal class B and add a new filed in A which is an hashmap[String, Object] containing a B
    a.getHmo().put("ccc", new B("Marge", "v4"));

    //    a.hmo.put("bla", new B("Bart", "v2"));
    //    a.hmo.put("boo", "Homer as String");
    //    a.hmo.put("answer", 42);

    //    a.mm.put("bla", new B("Bart", "v2"));
    //    a.mm.put("bla", "Homer as String");
    //    a.mm.put("answer", 42);
    //    a.mm.put("answer", "no");

    write(13, a, schema);

    read(1, schema);
    read(2, schema);
    read(3, schema);
    read(4, schema);
    read(5, schema);
    read(6, schema);
    read(7, schema);
    read(8, schema);
    read(9, schema);
    read(10, schema);
    read(11, schema);
    read(12, schema);
    read(13, schema);
    //    read(14, schema);
  }

  private static void write(int version, A a, Schema<A> schema) throws IOException {
    String dir = System.getProperty("user.home") + "/tmp/TestProtostuff/src/main/resources/";
    try (OutputStream os = new FileOutputStream(dir + "a_v" + version + ".serps")) {
      LinkedBuffer linkedBuffer = LinkedBuffer.allocate();
      ProtobufIOUtil.writeTo(os, a, schema, linkedBuffer);
    }
  }

  private static void read(int version, Schema<A> schema) throws IOException {
    String dir = System.getProperty("user.home") + "/tmp/TestProtostuff/src/main/resources/";
    try (InputStream io = new FileInputStream(dir + "a_v" + version + ".serps")) {
      A ret = schema.newMessage();
      ProtobufIOUtil.mergeFrom(io, ret, schema);
      System.out.println("Read version " + version + " => " + ret);
    }
  }

  private static class ListSupplier<T> implements Serializable, Supplier<List<T>> {
    private static final long serialVersionUID = 0xfb4eb5b;

    @Override
    public List<T> get() {
      return new LinkedList<>();
    }
  }

  private static final Supplier<List<Object>> listSupplier = new ListSupplier<>();

  @Data
  static class A {
    private String name;
    //    private int version;
    private List<String> listOfStrings = new java.util.ArrayList<>();
    private B b;
    private Map<String, B> bs = new HashMap<>();
    private Map<String, Object> hmo = new HashMap<>();
    //    private ListMultimap<String, Object> mm = Multimaps.newListMultimap(new TreeMap<>(), listSupplier);

  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class B {
    private String name;
    //    private int id;
    private String version;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class WrapperMapBByString {
    private Map<String, B> mapBByString;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class WrapperObjectsCollection {
    private Collection<Object> objects;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  static class WrapperMapObjectByString {
    private Map<String, Object> mapObjectByString;
  }

  @Data
  static class WrapperMultimapObjectByString {
    public WrapperMultimapObjectByString(ListMultimap<String, Object> mm) {
      this.multimapObjectsCollectionByString = mm.asMap().entrySet()
          .stream()
          .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new WrapperObjectsCollection(e.getValue())))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    private Map<String, WrapperObjectsCollection> multimapObjectsCollectionByString;
    private ListMultimap<String, Object> asListMultimap() {
      ListMultimap<String, Object> ret = Multimaps.newListMultimap(new HashMap<>(), listSupplier);
      this.multimapObjectsCollectionByString.forEach((key, value) -> ret.putAll(key, value.objects));
      return ret;
    }
  }

  static class ASchema implements Schema<A> {
    private final static Schema<B> schemaB = new BSchema();
    private final static Schema<WrapperMapObjectByString> schemaMObS = RuntimeSchema.getSchema(WrapperMapObjectByString.class);
    private final static Schema<WrapperMapBByString> schemaBS = RuntimeSchema.getSchema(WrapperMapBByString.class);
    private final static Schema<WrapperMapObjectByString> schemaMOS = RuntimeSchema.getSchema(WrapperMapObjectByString.class);
    private final static Schema<WrapperMultimapObjectByString> schemaMM = RuntimeSchema.getSchema(WrapperMultimapObjectByString.class);
    static {
      RuntimeSchema.register(BSchema.class);
      RuntimeSchema.register(WrapperMapObjectByString.class);
    }

    private final static int INDEX_NAME = 1;
    private final static int INDEX_VERSION = 2;
    private final static int INDEX_LIST_OF_STRINGS = 3;
    private final static int INDEX_B = 4;
    private final static int INDEX_BS = 5;
    private final static int INDEX_HMO = 6;
    private final static int INDEX_MM = 7;

    @Override
    public String getFieldName(int i) {
      switch(i) {
      case INDEX_NAME: return "name";
      case INDEX_VERSION: return "version";
      case INDEX_LIST_OF_STRINGS: return "listOfStrings";
      case INDEX_B: return "b";
      case INDEX_BS: return "bs";
      case INDEX_HMO: return "hmo";
      case INDEX_MM: return "mm";
      default: return null;
      }
    }

    @Override
    public int getFieldNumber(String s) {
      switch(s) {
      case "name": return INDEX_NAME;
      case "version": return INDEX_VERSION;
      case "listOfStrings": return INDEX_LIST_OF_STRINGS;
      case "b": return INDEX_B;
      case "bs": return INDEX_BS;
      case "hmo": return INDEX_HMO;
      case "mm": return INDEX_MM;
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
        //          case INDEX_VERSION:
        //            a.version = input.readInt32();
        //            break;
        case INDEX_LIST_OF_STRINGS:
          a.getListOfStrings().add(input.readString());
          break;
        case INDEX_B:
          a.setB(input.mergeObject(null, schemaB));
          break;
        case INDEX_BS:
          a.bs = input.mergeObject(null, schemaBS).mapBByString;
          break;
        case INDEX_HMO:
          a.hmo.putAll(input.mergeObject(null, schemaMOS).mapObjectByString);
          break;
        //        case INDEX_MM:
        //          a.mm = input.mergeObject(null, schemaMM).asListMultimap();
        //          break;
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

      //      output.writeInt32(INDEX_VERSION, a.version, false);

      for (String s : a.getListOfStrings()) {
        output.writeString(INDEX_LIST_OF_STRINGS, s, true);
      }

      if (a.b != null) {
        output.writeObject(INDEX_B, a.getB(), schemaB, false);
      }

      output.writeObject(INDEX_BS, new WrapperMapBByString(a.bs), schemaBS, false);

      //      output.writeObject(INDEX_HMO, new WrapperMapObjectByString(a.hmo), schemaMOS, false);
//      output.writeObject(INDEX_HMO, a.hmo, schemaMOS, false);

      //      for (Map.Entry<String, Object > entry : a.hmo.entrySet()) {
      //        if (entry.getValue() instanceof B) {
      //          output.writeObject(INDEX_HMO, new WrapperMapEntryBByString(entry.getKey(), (B) entry.getValue()), schemaMEOS, true);
      //        } else
      //          throw new NotImplementedException("Unknow type of object to save: " + entry.getClass());
      //      }

      //      output.writeObject(INDEX_MM, new WrapperMultimapObjectByString(a.mm), schemaMM, false);
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
          b.setName(input.readString());
          break;
        //        case INDEX_ID:
        //          b.id = input.readInt32();
        //          break;
        case INDEX_VERSION:
          b.setVersion(input.readString());
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
      //      output.writeInt32(INDEX_ID, b.id, false);
      if (b.getVersion() != null) {
        output.writeString(INDEX_VERSION, b.getVersion(), false);
      }
    }
  }
}

