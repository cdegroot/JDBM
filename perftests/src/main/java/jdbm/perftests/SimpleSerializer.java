/*
 * Created on Oct 7, 2005
 * (c) 2005 Trumpet, Inc.
 *
 */
package jdbm.perftests;

import java.io.IOException;

import jdbm.helper.Serializer;

/**
 * A do-nothing serializer that takes minimal CPU time and allows creation of
 * records of a specific size.
 * @author kevin day (trumpetinc@sourceforge.net)
 */
class SimpleSerializer implements Serializer{
       int dataSize;
       
       public SimpleSerializer(int dataSize){
           this.dataSize = dataSize;
       }
       
       public byte[] serialize(Object obj) throws IOException {
//            we are just returning something to consume space
           return new byte[dataSize];
       }
       
       public Object deserialize(byte[] serialized) throws IOException {
//            There was nothing in the data anyway, so just create a new object and return it
           return new Object();
       }
       
   }