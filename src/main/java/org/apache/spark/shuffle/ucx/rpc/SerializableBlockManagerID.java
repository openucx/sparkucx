package org.apache.spark.shuffle.ucx.rpc;

import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import org.apache.spark.storage.BlockManagerId;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 * Static mthods to serialize BlockManagerID to ByteBuffer.
 */
public class SerializableBlockManagerID {

    public static void serializeBlockManagerID(BlockManagerId blockManagerId,
                                               ByteBuffer metadataBuffer) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(
                new ByteBufferOutputStream(metadataBuffer));
        blockManagerId.writeExternal(oos);
        oos.close();
    }

    static BlockManagerId deserializeBlockManagerID(ByteBuffer metadataBuffer) throws IOException {
        ObjectInputStream ois =
                new ObjectInputStream(new ByteBufferInputStream(metadataBuffer));
        BlockManagerId blockManagerId = BlockManagerId.apply(ois);
        ois.close();
        return blockManagerId;
    }
}
