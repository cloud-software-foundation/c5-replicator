package ohmdb;

import com.google.common.util.concurrent.Service;

/**
 * An internal service.  Extends guava services.
 */
public interface OhmService extends Service {

    public String getServiceName();
    public boolean hasPort();
    public int port();
}
