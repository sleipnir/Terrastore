/**
 * Copyright 2009 - 2011 Sergio Bossa (sergio.bossa@gmail.com)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package terrastore.util.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

/**
 * @author Sergio Bossa
 * @author Adriano Santos
 */
public class MsgPackSerializer<T> implements Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MsgPackSerializer.class);
    //
    private final boolean compressed;
    
    private final MessagePack msgpack = new MessagePack();


    public MsgPackSerializer(boolean compressed) {
        this.compressed = compressed;
    }

    @Override
    public byte[] serialize(T object) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        OutputStream stream = null;
        if (compressed) {
            stream = new LZFOutputStream(bytes);
        } else {
            stream = bytes;
        }
        doSerialize(object, stream);
        return bytes.toByteArray();
    }

    @Override
    public T deserialize(byte[] serialized) {
        return deserialize(new ByteArrayInputStream(serialized));
    }

    @Override
    public T deserialize(InputStream serialized) {
        if (IOUtils.isCompressed(serialized)) {
            try {
                return doDeserialize(new LZFInputStream(serialized));
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
                throw new RuntimeException(ex.getMessage(), ex);
            }
        } else {
            return doDeserialize(serialized);
        }
    }

    private void doSerialize(T object, OutputStream stream) {
        try {
            Packer packer = msgpack.createPacker(stream);
            packer.write(object.getClass().getName());
            packer.write(object);
            stream.flush();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    private T doDeserialize(InputStream stream) {
        try {
            Unpacker unpacker = msgpack.createUnpacker(stream);
            String className = unpacker.readString();
            return unpacker.read((Class<T>) Class.forName(className));
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

}
