/**
 * 
 */
package jdbm.helper.compression;

import java.io.DataInput;
import java.io.IOException;

/**
 * Interface that provides for decompressing of groups of byte arrays
 * that were compressed using @link ByteArrayGroupCompressor.
 * 
 * Note that implementors of this interface will probably need to store
 * state from one call to the next, so it is NOT safe to use a single instance
 * of these objects for multiple purposes without calling reset().
 * 
 * 
 * 
 * @author kevin
 *
 */
public interface ByteArrayDecompressor {
	/**
	 * Resets the internal state of the decompressor so it can be used again, and sets
	 * the input source for the decompressor to be @param in.
	 * @param in The new input source for the compressor
	 */
	void reset(DataInput in);

	/**
	 * Returns the next group from the input source.
	 * @return The next group from the input source.
	 * @throws IOException
	 */
	byte[] decompressNextGroup() throws IOException;

}
