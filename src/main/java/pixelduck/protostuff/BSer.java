package pixelduck.protostuff;

import java.io.IOException;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Pipe;
import io.protostuff.Schema;
import io.protostuff.UninitializedMessageException;
import pixelduck.pojo.B;

/**
 * Definition of schema for class B.
 * The two schemas should be updated.
 */
public final class BSer {
  private BSer() {
  }

  final static int INDEX_NAME = 1;
  final static int INDEX_VERSION = 2;
  final static int INDEX_AUTHOR = 3;

  public static final Schema<B> B_SCHEMA = new Schema<B>() {

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
        case INDEX_VERSION:
          b.setVersion(input.readInt32());
          break;
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
      output.writeInt32(INDEX_VERSION, b.getVersion(), false);
      output.writeString(INDEX_AUTHOR, b.getAuthor(), false);
    }
  };

  public static final Pipe.Schema<B> B_PIPE_SCHEMA = new Pipe.Schema<B>(B_SCHEMA) {
    @Override
    protected void transfer(Pipe pipe, Input input, Output output) throws IOException {
      for (int number = input.readFieldNumber(wrappedSchema);; number = input
          .readFieldNumber(wrappedSchema))
      {
        switch (number)
        {
        case 0:
          return;
        case INDEX_NAME:
          output.writeString(number, input.readString(), false);
          break;
        case INDEX_VERSION:
          output.writeUInt32(number, input.readUInt32(), false);
          break;
        case INDEX_AUTHOR:
          output.writeString(number, input.readString(), false);
          break;
        default:
          input.handleUnknownField(number, this);
        }
      }
    }
  };
}
