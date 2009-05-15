/*
 *  Primitive Collections for Java.
 *  Copyright (C) 2002, 2003  S�ren Bak
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package jdbm.helper.maps;

import java.io.Serializable;

/**
 *  This class provides a default hash function for
 *  long values. It has been derived from the Java library and
 *  is known to work well in the general case.
 *
 *  @see        Long#hashCode()
 *
 *  @serial     exclude
 *  @author     S&oslash;ren Bak
 *  @version    1.2     2002/5/3
 *  @since      1.0
 *  
 *  ********* JDBM Project Note *************
 *  This class was extracted from the pcj project (with permission)
 *  for use in jdbm only.  Modifications to original were performed
 *  by Kevin Day to make it work outside of the pcj class structure. 
 *  
 */
public class DefaultLongHashFunction implements LongHashFunction, Serializable {

	private static final long serialVersionUID = -6132950247289330579L;
	
	/** Default instance of this hash function. */
    public static final LongHashFunction INSTANCE = new DefaultLongHashFunction();

    /** Default constructor to be invoked by sub-classes. */
    protected DefaultLongHashFunction() { }

    public int hash(long v) {
        return (int)(v ^ (v >>> 32));
    }

}