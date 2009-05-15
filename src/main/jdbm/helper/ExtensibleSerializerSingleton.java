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
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: ExtensibleSerializerSingleton.java,v 1.3 2006/05/29 19:37:08 thompsonbry Exp $
 */
/*
 * Created on Nov 4, 2005
 */
package jdbm.helper;

import java.io.IOException;

import org.CognitiveWeb.extser.AbstractSingleton;
import org.CognitiveWeb.extser.IExtensibleSerializer;

import jdbm.RecordManager;

/**
 * Stateless singleton seralizer wrapping the semantics of the {@link
 * ExtensibleSerializer} serializer. The use of this class prevents multiple
 * copies of the state of the extensible serializer from being written into the
 * store.
 * <p>
 * 
 * The use of the {@link IExtensibleSerializer} is selected by the sticky
 * {@link RecordManagerOptions#SERIALIZER} configuration option and specifying
 * the {@link RecordManagerOptions#SERIALIZER_EXTENSIBLE} as the value of that
 * property.
 * <p>
 * 
 * @author thompsonbry
 * @version $Id: ExtensibleSerializerSingleton.java,v 1.3 2006/05/29 19:37:08 thompsonbry Exp $
 */

public class ExtensibleSerializerSingleton
    extends AbstractSingleton
    implements ISerializationHandler
{

	private static final long serialVersionUID = 5946543932541728877L;

	/**
     * Return the {@link ExtensibleSerializer} singleton for the store.<p>
     * 
     * @exception IllegalStateException if the serializer has not been
     * set for the store.
     * 
     * @see #setSerializer( RecordManager recman, ExtensibleSerializer ser )
     */

    public ExtensibleSerializer getSerializer( RecordManager recman )
        throws IllegalStateException
    {
        
        return (ExtensibleSerializer) super.getSerializer
            ( recman.getBaseRecordManager()
              );
        
    }

    /**
     * The {@link RecordManagerProvider}is responsible for inserting or
     * fetching this object from the store and invoking this method with that
     * instance. Thereafter the serializer reference is maintained as part of a
     * static transient cache managed by this class.
     * <p>
     * 
     * @param recman
     *            The record manager.
     * 
     * @param ser
     *            The serializer.
     * 
     * @exception IllegalStateException
     *                If the serializer has already been set for that record
     *                manager.
     */

    public void setSerializer( RecordManager recman, ExtensibleSerializer ser )
        throws IllegalStateException
    {

        super.setSerializer
            ( recman.getBaseRecordManager(),
              ser
              );
        
    }

    //
    // ISerializationHandler
    //
    
    public byte[] serialize(RecordManager recman, long recid, Object obj) throws IOException {
        return getSerializer(recman).serialize( recid, obj );
    }

    public Object deserialize(RecordManager recman, long recid, byte[] serialized) throws IOException {
        return getSerializer(recman).deserialize( recid, serialized );
    }

}
