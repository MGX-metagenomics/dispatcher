package de.cebitec.mgx.dispatcher.web;

import de.cebitec.mgx.dispatcher.JobReceiver;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import de.cebitec.mgx.dispatcher.web.exception.MGXWebException;
import java.util.UUID;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 * @author sjaenick
 */
@Stateless
@Path("Job")
public class JobBean {

    @EJB
    JobReceiver receiver;

    @GET
    @Path("validate/{projClass}/{projName}/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean validate(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXDispatcherException {
        return receiver.validate(projClass, projName, jobId);
    }

    @GET
    @Path("submit/{projClass}/{projName}/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean submit(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXDispatcherException {
        return receiver.submit(projClass, projName, jobId);
    }

    @DELETE
    @Path("cancel/{projClass}/{projName}/{id}")
    public Response cancel(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXWebException {
        try {
            receiver.cancel(projClass, projName, jobId);
        } catch (MGXDispatcherException ex) {
            throw new MGXWebException(ex.getMessage());
        }
        return Response.ok().build();
    }

    @DELETE
    @Path("delete/{projClass}/{projName}/{id}")
    public Response delete(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXDispatcherException {
        receiver.delete(projClass, projName, jobId);
        return Response.ok().build();
    }

    @GET
    @Path("shutdown/{uuid}")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean shutdown(@PathParam("uuid") String uuid) {
        try { 
            UUID token = UUID.fromString(uuid);
            return receiver.shutdown(UUID.fromString(uuid));
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }
}
