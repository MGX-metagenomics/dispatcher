/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cebitec.mgx.dispatcher.web;

import java.util.Set;
import javax.ws.rs.core.Application;

/**
 *
 * @author sj
 */
@javax.ws.rs.ApplicationPath("webresources")
public class AppConfig extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> resources = new java.util.HashSet<>();
        addRestResourceClasses(resources);
        return resources;
    }

    private void addRestResourceClasses(Set<Class<?>> resources) {
        resources.add(de.cebitec.mgx.dispatcher.web.JobBean.class);
        resources.add(de.cebitec.mgx.dispatcher.web.StringTextPlainWriter.class);
        resources.add(de.cebitec.mgx.dispatcher.web.TextPlainWriter.class);
        resources.add(de.cebitec.mgx.dispatcher.web.exception.MGXJobExceptionMapper.class);
        resources.add(de.cebitec.mgx.dispatcher.web.exception.MGXWebExceptionMapper.class);
    }
    
}