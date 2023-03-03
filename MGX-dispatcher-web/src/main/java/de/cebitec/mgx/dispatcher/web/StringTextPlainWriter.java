package de.cebitec.mgx.dispatcher.web;

import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyWriter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

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
