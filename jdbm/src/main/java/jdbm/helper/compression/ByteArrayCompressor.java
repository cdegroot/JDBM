package jdbm.helper.compression;

import java.io.IOException;

/**
 * Interface that provides for compressing of groups of byte arrays.
 * Implementors could use leading common value or prior leading common 
 * value strategies for compressing the results.
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

public interface ByteArrayCompressor {

	/**
	 * Adds the specified input bytes to the compressor.  There is no guarantee that
	 * all (or any) data will be written to the output (some compressors will need to
	 * cache data over multiple groups in order to perform effective compression).
	 * 
	 * If null is passed in for @param in, the compressor must store sufficient information
	 * for the decompressor to return null when decompressing that group.
	 * 
	 * @param out
	 * @param in
	 * @throws IOException
	 */
	void compressNextGroup(byte[] in) throws IOException;
	
	/**
	 * Causes the compressor to finish writing it's data to the output.  After this call, all
	 * data will be written to the output, and the compressor can be reused by calling reset().
	 * @param out
	 */
	void finishCompression();
	
	
}
