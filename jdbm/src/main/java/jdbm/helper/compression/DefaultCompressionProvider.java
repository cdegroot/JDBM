package jdbm.helper.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.CognitiveWeb.extser.Stateless;


/**
 * Provider for default implementation of the ByteArrayGroupCompressor and ByteArrayGroupDecompressor that just
 * store the byte array groups as is.  These class is
 * designed to be compatible with earlier (1.0) storage implementations of
 * keys in org.jdbm.btree.BPage's serialize and deserialize methods.
 * 
 * @author kevin
 *
 */

public class DefaultCompressionProvider implements CompressionProvider, Stateless {
	private static final long serialVersionUID = 1L;

	public DefaultCompressionProvider() {
	}

	public ByteArrayCompressor getCompressor(DataOutput out) {
		Compressor comp = new Compressor();
		comp.out = out;
		return comp;
	}
	public ByteArrayDecompressor getDecompressor(DataInput in) {
		Decompressor decomp = new Decompressor();
		decomp.in = in;
		return decomp;
	}
	
	
    static byte[] readByteArray( DataInput in ) throws IOException
	{
	    int len = in.readInt();
	    if ( len < 0 ) {
	        return null;
	    }
	    byte[] buf = new byte[ len ];
	    in.readFully( buf );
	    return buf;
	}
	
	
	static private class Compressor implements ByteArrayCompressor{
		private DataOutput out;

		public void compressNextGroup(byte[] in) throws IOException {
			writeByteArray(out, in);
		}

		public void finishCompression() {
			// not needed in this implementation
		}
		
	}
	
	static private class Decompressor implements ByteArrayDecompressor{
		private DataInput in;

		public void reset(DataInput in) {
			this.in = in;
		}

		public byte[] decompressNextGroup() throws IOException {
			return readByteArray(in);
		}
		
	}
    
	static void writeByteArray( DataOutput out, byte[] buf ) throws IOException
	{
	    if ( buf == null ) {
	        out.writeInt( -1 );
	    } else {
	        out.writeInt( buf.length );
	        out.write( buf );
	    }
	}

}
