package org.eclipse.che.api.environment.server.exception;

/**
 * Thrown when environment start is interrupted.
 *
 * @author Yevhenii Voevodin
 */
public class EnvironmentStartInterruptedException extends EnvironmentException {
    public EnvironmentStartInterruptedException(String workspaceId, String envName) {
        super(String.format("Start of environment '%s' in workspace '%s' is interrupted",
                            envName,
                            workspaceId));
    }
}
