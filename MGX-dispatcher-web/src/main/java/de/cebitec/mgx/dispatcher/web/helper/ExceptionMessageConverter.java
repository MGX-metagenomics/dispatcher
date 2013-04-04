
package de.cebitec.mgx.dispatcher.web.helper;

/**
 *
 * @author sjaenick
 */
public class ExceptionMessageConverter {

    public static String convert(String message) {
        String template = "java.sql.SQLException: ";
        int templateStartPosition = message.indexOf(template);
        if (templateStartPosition < 0) {
            return message;
        }
        int start = templateStartPosition + template.length();
        return message.substring(start, message.length());

    }
}
