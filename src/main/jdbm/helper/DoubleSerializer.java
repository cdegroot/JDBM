/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 * $Id: DoubleSerializer.java,v 1.2 2006/05/03 19:51:33 thompsonbry Exp $
 */
/*
 * Created on Sep 29, 2005
 */
package jdbm.helper;

import java.io.IOException;

import org.CognitiveWeb.extser.Stateless;


/**
 * Optimized (de-)serialization for {@link Double}.
 */

public class DoubleSerializer
	implements Serializer, Stateless
{

    static transient public final Serializer INSTANCE = new DoubleSerializer();

    public byte[] serialize(Object obj) throws IOException {
        // Note: use of "raw" bits preserves NaN types.
        long bits = Double.doubleToRawLongBits( ((Double)obj).doubleValue() );
//        byte[] data = new byte[8];
//        Conversion.pack8( data, 0, bits );
//        return data;
        return Conversion.convertToByteArray( bits );
    }

    public Object deserialize(byte[] serialized) throws IOException {
        // Note: "raw" bits are converted to a double using the same
        // method as "cooked" bits.
//        long bits = Conversion.unpack8(serialized, 0);
        long bits = Conversion.convertToLong( serialized );
        return new Double( Double.longBitsToDouble( bits ) );
    }

    private static final long serialVersionUID = 4148669273975369802L;
    
}
