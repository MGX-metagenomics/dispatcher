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
public class TextPlainWriter implements MessageBodyWriter<Boolean> {

    @Override
    public boolean isWriteable(Class<?> type, Type type1, Annotation[] antns, MediaType mt) {
        return type == Boolean.class;
    }

    @Override
    public long getSize(Boolean t, Class<?> type, Type type1, Annotation[] antns, MediaType mt) {
        return -1;
    }
    
    private byte[] buf = new byte[1];

    @Override
    public void writeTo(Boolean t, Class<?> type, Type type1, Annotation[] antns, MediaType mt, MultivaluedMap<String, Object> mm, OutputStream out) throws IOException, WebApplicationException {
        buf[0] = '0';
        if (t) {
            buf[0] = '1';
        }
        out.write(buf);
    }
}
