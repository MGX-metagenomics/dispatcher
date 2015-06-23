package de.cebitec.mgx.dispatcher.web;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

/**
 *
 * @author sjaenick
 */
@Provider
@Produces(MediaType.TEXT_PLAIN)
public class StringTextPlainWriter implements MessageBodyWriter<String> {

    @Override
    public boolean isWriteable(Class<?> type, Type type1, Annotation[] antns, MediaType mt) {
        return type == String.class;
    }

    @Override
    public long getSize(String t, Class<?> type, Type type1, Annotation[] antns, MediaType mt) {
        return -1;
    }

    @Override
    public void writeTo(String t, Class<?> type, Type type1, Annotation[] antns, MediaType mt, MultivaluedMap<String, Object> mm, OutputStream out) throws IOException, WebApplicationException {
        byte[] bytes = t.getBytes();
        out.write(bytes);
        if (bytes[bytes.length-1] != '\n') {
            out.write('\n');
        }
    }
}
