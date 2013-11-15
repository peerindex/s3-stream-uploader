package net.peerindex.s3stream;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public abstract class VerboseCallable<T> implements Callable<T> {
    private static final Logger log = LoggerFactory.getLogger(VerboseCallable.class);

    @Override
    public final T call() throws Exception {
        try {
            return doCall();
        } catch (Exception e) {
            throw e;
        } catch (Throwable e) {
            log.error("Uncaught throwable while executing " + this.getClass().getSimpleName(), e);
            throw Throwables.propagate(e);
        }
    }

    protected abstract T doCall() throws Exception;
}
