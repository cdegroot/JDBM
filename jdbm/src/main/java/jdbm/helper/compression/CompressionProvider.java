package jdbm.helper.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;

public interface CompressionProvider extends Serializable {
	public ByteArrayCompressor getCompressor(DataOutput out);
	public ByteArrayDecompressor getDecompressor(DataInput in);
}
