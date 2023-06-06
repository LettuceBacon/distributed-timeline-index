package xyz.mfj.utils;

import java.util.Base64;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerDeUtil {
    private static Kryo kryo = new Kryo();
    private static int outputBufferSize = 8192;
    private static int maxOutputBufferSize = 67108864;
    
    public static String serialize(Object obj) {
        Output out = new Output(outputBufferSize, maxOutputBufferSize);
        kryo.writeObject(out, obj);
        return Base64.getMimeEncoder().encodeToString(out.toBytes());
    }
    
    public static <T> T deserialize(String str, Class<T> clazz) {
        byte[] sargBytes = Base64.getMimeDecoder().decode(str);
        return kryo.readObject(new Input(sargBytes), clazz);
    }
    
}
