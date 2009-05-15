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
 */

package jdbm.helper;

import java.io.IOException;

import org.CognitiveWeb.extser.Stateless;


/**
 * Optimized serializer for long integers.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: LongSerializer.java,v 1.5 2006/05/03 19:51:33 thompsonbry Exp $
 */
public class LongSerializer 
    implements Serializer, Stateless
{

	private static final long serialVersionUID = -7980451649962070585L;
	
	public transient static final LongSerializer INSTANCE = new LongSerializer();
    
    
    /**
     * Construct a LongSerializer.
     */
    public LongSerializer()
    {
        // no op
    }

    
    /**
     * Serialize the content of an object into a byte array.
     *
     * @param obj Object to serialize
     * @return a byte array representing the object's state
     */
     public byte[] serialize( Object obj )
        throws IOException
     {
         Long number = (Long) obj;
         return Conversion.convertToByteArray( number.longValue() );
     }
        
        
    /**
     * Deserialize the content of an object from a byte array.
     *
     * @param serialized Byte array representation of the object
     * @return deserialized object
     */
     public Object deserialize( byte[] serialized )
        throws IOException
     {
         long number = Conversion.convertToLong( serialized );
         return new Long( number );
     }

}
