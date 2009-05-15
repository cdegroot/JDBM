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
 * $Id: CachePolicyListener.java,v 1.4 2006/05/03 16:01:18 thompsonbry Exp $
 */

package jdbm.helper;

/**
 * Callback interface between {@link CachePolicy} and a Cache implementation
 * to notify about cached object eviction.
 * <p>
 * Note that <code>CachePolicy</code> implementations typically use
 * <em>object equality</em> when removing listeners, so concrete
 * implementations of this interface should also pay attention to
 * their {@link Object#equals(Object)} and {@link Object#hashCode()}
 * methods.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: CachePolicyListener.java,v 1.4 2006/05/03 16:01:18 thompsonbry Exp $
 */
public interface CachePolicyListener {

    /**
     * Notification that the cache this listener is attached to is evicting
     * the object indicated.
     *
     * @param key the key under which that object was recorded in the cache.
     * @param obj object being evited from cache
     * @param dirty true iff the object state was dirty.
     * @param ser the object to be used to serialize the state of the evicted
     * object or <code>null</code> to use the default serializer configured for
     * the record manager.
     * 
     * @throws CacheEvictionException if this listener encountered problems
     *     while preparing for the specified object's eviction. For example,
     *     a listener may try to persist the object to disk, and encounter
     *     an <code>IOException</code>.
     */
    public void cacheObjectEvicted(Object key, Object obj, boolean dirty, Serializer ser ) throws CacheEvictionException;

}
