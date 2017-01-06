package org.eclipse.che.api.environment.server;

import org.eclipse.che.api.core.ServerException;
import org.eclipse.che.api.machine.server.spi.Instance;

/**
 * Used in couple with {@link CheEnvironmentEngine#start} method to
 * allow sequential handling and interruption of the start process.
 *
 * <p>This interface is a part of a contract for {@link CheEnvironmentEngine}.
 *
 * @author Yevhenii Voevodin
 */
public interface MachineStartedHandler {
    void started(Instance machine) throws ServerException;
}
