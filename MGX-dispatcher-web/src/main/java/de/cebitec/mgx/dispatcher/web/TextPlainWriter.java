package de.cebitec.mgx.dispatcher.web;

import java.io.ByteArrayOutputStream;
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
public class TextPlainWriter implements MessageBodyWriter<Boolean> {

    @Override
    public boolean isWriteable(Class<?> type, Type type1, Annotation[] antns, MediaType mt) {
        return true;
    }

    @Override
    public long getSize(Boolean t, Class<?> type, Type type1, Annotation[] antns, MediaType mt) {
        return 1;
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
