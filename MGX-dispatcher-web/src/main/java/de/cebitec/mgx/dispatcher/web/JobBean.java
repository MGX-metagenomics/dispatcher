package de.cebitec.mgx.dispatcher.web;

import de.cebitec.mgx.dispatcher.api.JobReceiverI;
import de.cebitec.mgx.dispatcher.common.api.MGXDispatcherException;
import de.cebitec.mgx.dispatcher.web.exception.MGXWebException;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.UUID;

/**
 *
 * @author sjaenick
 */
@Stateless
@Path("Job")
public class JobBean {

    @EJB
    JobReceiverI receiver;

    @GET
    @Path("validate/{projClass}/{projName}/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean validate(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXWebException {
        return receiver.validate(projClass, projName, jobId);
    }

    @GET
    @Path("submit/{projClass}/{projName}/{id}")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean submit(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXWebException {
        try {
            return receiver.submit(projClass, projName, jobId);
        } catch (MGXDispatcherException ex) {
            throw new MGXWebException(ex.getMessage());
        }
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
    public Response delete(@PathParam("projClass") String projClass, @PathParam("projName") String projName, @PathParam("id") long jobId) throws MGXWebException {
        try {
            receiver.delete(projClass, projName, jobId);
        } catch (MGXDispatcherException ex) {
            throw new MGXWebException(ex.getMessage());
        }
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
