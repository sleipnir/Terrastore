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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import terrastore.cluster.ensemble.impl.View;
import terrastore.common.ErrorMessage;
import terrastore.communication.NodeConfiguration;
import terrastore.store.Key;
import terrastore.store.Value;
import terrastore.store.features.Mapper;
import terrastore.store.features.Predicate;
import terrastore.store.features.Range;
import terrastore.store.features.Reducer;
import terrastore.store.features.Update;

/**
 * @author Sergio Bossa
 * @author Adriano Santos
 */
public class MsgPackUtils {

    private static final JavaSerializer<Object> HELPER = new JavaSerializer<Object>();

    public static void packBoolean(Packer packer, boolean value) throws IOException {
        packer.write(value);
    }

    public static void packBytes(Packer packer, byte[] value) throws IOException {
        packer.write(value);
    }

    public static void packInt(Packer packer, int value) throws IOException {
        packer.write(value);
    }

    public static void packLong(Packer packer, long value) throws IOException {
        packer.write(value);
    }

    public static void packString(Packer packer, String value) throws IOException {
        packer.write(value);
    }

    public static void packKey(Packer packer, Key key) throws IOException {
        if (key != null) {
            packer.write(key);
        } else {
            packer.writeNil();
        }
    }

    public static void packValue(Packer packer, Value value) throws IOException {
        if (value != null) {
            packer.write(value);
        } else {
            packer.writeNil();
        }
    }

    public static void packErrorMessage(Packer packer, ErrorMessage errorMessage) throws IOException {
        if (errorMessage != null) {
            packer.write(errorMessage);
        } else {
            packer.writeNil();
        }
    }

    public static void packKeys(Packer packer, Set<Key> keys) throws IOException {
        if (keys != null) {
            packer.write(keys.size());
            for (Key key : keys) {
                packKey(packer, key);
            }
        } else {
            packer.writeNil();
        }
    }

    public static void packValues(Packer packer, Map<Key, Value> values) throws IOException {
        if (values != null) {
            packer.write(values.size());
            for (Map.Entry<Key, Value> entry : values.entrySet()) {
                packKey(packer, entry.getKey());
                packValue(packer, entry.getValue());
            }
        } else {
            packer.writeNil();
        }
    }

    public static void packGenericMap(Packer packer, Map<String, Object> genericMap) throws IOException {
        if (genericMap != null) {
            packer.write(HELPER.serialize(genericMap));
        } else {
            packer.writeNil();
        }
    }

    @SuppressWarnings("rawtypes")
	public static void packGenericSet(Packer packer, Set genericSet) throws IOException {
        if (genericSet != null) {
            packer.write(HELPER.serialize(genericSet));
        } else {
            packer.writeNil();
        }
    }

    public static void packMapper(Packer packer, Mapper mapper) throws IOException {
        if (mapper != null) {
            packer.write(mapper);
        } else {
            packer.writeNil();
        }
    }

    public static void packReducer(Packer packer, Reducer reducer) throws IOException {
        if (reducer != null) {
            packer.write(reducer);
        } else {
            packer.writeNil();
        }
    }

    public static void packPredicate(Packer packer, Predicate predicate) throws IOException {
        if (predicate != null) {
            packer.write(predicate);
        } else {
            packer.writeNil();
        }
    }

    public static void packRange(Packer packer, Range range) throws IOException {
        if (range != null) {
            packer.write(range);
        } else {
            packer.writeNil();
        }
    }

    public static void packUpdate(Packer packer, Update update) throws IOException {
        if (update != null) {
            packer.write(update);
        } else {
            packer.writeNil();
        }
    }

    public static void packServerConfiguration(Packer packer, NodeConfiguration serverConfiguration) throws IOException {
        if (serverConfiguration != null) {
            packer.write(serverConfiguration);
        } else {
            packer.writeNil();
        }
    }

    public static void packView(Packer packer, View view) throws IOException {
        if (view != null) {
            packer.write(view);
        } else {
            packer.writeNil();
        }
    }

    public static boolean unpackBoolean(Unpacker unpacker) throws IOException {
        return unpacker.readBoolean();
    }

    public static byte[] unpackBytes(Unpacker unpacker) throws IOException {
        return unpacker.readByteArray();
    }

    public static int unpackInt(Unpacker unpacker) throws IOException {
        return unpacker.readInt();
    }

    public static long unpackLong(Unpacker unpacker) throws IOException {
        return unpacker.readLong();
    }

    public static String unpackString(Unpacker unpacker) throws IOException {
        return unpacker.readString();
    }

    public static Key unpackKey(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Key.class);
        }
    }

    public static Value unpackValue(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Value.class);
        }
    }

    public static ErrorMessage unpackErrorMessage(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(ErrorMessage.class);
        }
    }

    public static Set<Key> unpackKeys(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            int size = unpackInt(unpacker);
            Set<Key> keys = new LinkedHashSet<Key>();
            for (int i = 0; i < size; i++) {
                keys.add(unpackKey(unpacker));
            }
            return keys;
        }
    }

    public static Map<Key, Value> unpackValues(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            int size = unpackInt(unpacker);
            Map<Key, Value> values = new LinkedHashMap<Key, Value>();
            for (int i = 0; i < size; i++) {
                values.put(unpackKey(unpacker), unpackValue(unpacker));
            }
            return values;
        }
    }

    @SuppressWarnings("unchecked")
	public static Map<String, Object> unpackGenericMap(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return (Map<String, Object>) HELPER.deserialize(unpacker.readByteArray());
        }
    }

    @SuppressWarnings("rawtypes")
	public static Set unpackGenericSet(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return (Set) HELPER.deserialize(unpacker.readByteArray());
        }
    }

    public static Mapper unpackMapper(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Mapper.class);
        }
    }

    public static Reducer unpackReducer(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Reducer.class);
        }
    }

    public static Predicate unpackPredicate(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Predicate.class);
        }
    }

    public static Range unpackRange(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Range.class);
        }
    }

    public static Update unpackUpdate(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(Update.class);
        }
    }

    public static NodeConfiguration unpackServerConfiguration(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(NodeConfiguration.class);
        }
    }

    public static View unpackView(Unpacker unpacker) throws IOException {
        if (unpacker.trySkipNil()) {
            return null;
        } else {
            return unpacker.read(View.class);
        }
    }
}
