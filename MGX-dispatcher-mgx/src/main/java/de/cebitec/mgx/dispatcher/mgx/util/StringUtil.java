/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.cebitec.mgx.dispatcher.mgx.util;

/**
 *
 * @author sj
 */
public class StringUtil {

    private StringUtil() {
    }
    
    public static String join(String[] elems, String separator) {
        if (elems == null || elems.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(elems[0]);
        for (int i = 1; i < elems.length; i++) {
            sb.append(separator);
            sb.append(elems[i]);
        }
        return sb.toString();
    }

}
