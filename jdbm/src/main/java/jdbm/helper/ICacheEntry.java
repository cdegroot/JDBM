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
 * $Id: ICacheEntry.java,v 1.2 2006/06/03 18:22:46 thompsonbry Exp $
 */
/*
 * Created on May 3, 2006
 */
package jdbm.helper;

/**
 * <p>
 * Interface implemented by entries in an {@link CachePolicy object cache}.
 * This interface exposes the persistent record identifier, the application
 * object, and dirty flag and serializer metadata associated with that object.
 * </p>
 * <p>
 * Note: Since dirty objects are installed when they are evicted from the hard
 * reference cache, it is impossible for there to be a dirty object on the weak
 * reference cache that is not also present in the hard reference cache.
 * Therefore the dirty flag and serializer are <em>only</em> maintained by the
 * backing hard reference cache. The various iterators are all defined to visit
 * the entries or objects in the backing hard reference cache when a weak
 * reference cache is used, so you never see the cache entries from the weak
 * reference cache.
 * </p>
 * 
 * @version $Id: ICacheEntry.java,v 1.2 2006/06/03 18:22:46 thompsonbry Exp $
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public interface ICacheEntry
{

    /**
     * The persistent record identifier (key) under which the object was
     * inserted into the cache.
     */
    public Object getKey();
    
    /**
     * The application object that was inserted into the cache under that key.
     * <p>
     * Note: For soft or weak reference caches a <code>null</code> return
     * indicates that the reference has been cleared.
     */
    public Object getValue();

    /**
     * True iff the state of the application object has been modified and needs
     * to be persisted against the store.
     */
    public boolean isDirty();
    
    /**
     * Set the dirty flag.
     * 
     * @param dirty
     *            The new value for the dirty flag.
     */
    public void setDirty(boolean dirty);
    
    /**
     * The object that will be used to serialize the application object or
     * <code>null</code> if the default serialization handler configured on
     * the record manager should be used.
     */
    public Serializer getSerializer();

}
