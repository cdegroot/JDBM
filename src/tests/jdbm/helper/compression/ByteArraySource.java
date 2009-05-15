/**
 * 
 */
package jdbm.helper.compression;

import java.util.Random;

public class ByteArraySource{
	byte[] last = new byte[0];
	Random r;
	
	public ByteArraySource(long seed){
		r = new Random(seed);
		r.nextBytes(last);
	}
	
	public byte[] getBytesWithCommonPrefix(int len, int common){
		if (common > last.length) common = last.length;
		if (common > len) common = len;
		
		byte[] out = new byte[len];
		System.arraycopy(last, 0, out, 0, common);
		byte[] xtra = new byte[len-common];
		r.nextBytes(xtra);
		System.arraycopy(xtra, 0, out, common, xtra.length);

		last = out;
		return out;
	}
	
}