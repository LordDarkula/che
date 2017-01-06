/*******************************************************************************
 * Copyright (c) 2012-2017 Codenvy, S.A.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Codenvy, S.A. - initial API and implementation
 *******************************************************************************/
package org.eclipse.che.api.workspace.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.eclipse.che.api.agent.server.AgentRegistry;
import org.eclipse.che.api.agent.server.exception.AgentException;
import org.eclipse.che.api.agent.server.impl.AgentSorter;
import org.eclipse.che.api.agent.server.launcher.AgentLauncher;
import org.eclipse.che.api.agent.server.launcher.AgentLauncherFactory;
import org.eclipse.che.api.agent.shared.model.Agent;
import org.eclipse.che.api.agent.shared.model.AgentKey;
import org.eclipse.che.api.core.ConflictException;
import org.eclipse.che.api.core.NotFoundException;
import org.eclipse.che.api.core.ServerException;
import org.eclipse.che.api.core.model.machine.MachineConfig;
import org.eclipse.che.api.core.model.workspace.Environment;
import org.eclipse.che.api.core.model.workspace.ExtendedMachine;
import org.eclipse.che.api.core.model.workspace.Workspace;
import org.eclipse.che.api.core.model.workspace.WorkspaceRuntime;
import org.eclipse.che.api.core.model.workspace.WorkspaceStatus;
import org.eclipse.che.api.core.notification.EventService;
import org.eclipse.che.api.core.util.MessageConsumer;
import org.eclipse.che.api.core.util.WebsocketMessageConsumer;
import org.eclipse.che.api.environment.server.CheEnvironmentEngine;
import org.eclipse.che.api.environment.server.MachineStartedHandler;
import org.eclipse.che.api.environment.server.exception.EnvironmentException;
import org.eclipse.che.api.environment.server.exception.EnvironmentNotRunningException;
import org.eclipse.che.api.environment.server.exception.EnvironmentStartInterruptedException;
import org.eclipse.che.api.machine.server.exception.MachineException;
import org.eclipse.che.api.machine.server.exception.SnapshotException;
import org.eclipse.che.api.machine.server.model.impl.MachineConfigImpl;
import org.eclipse.che.api.machine.server.model.impl.MachineImpl;
import org.eclipse.che.api.machine.server.model.impl.SnapshotImpl;
import org.eclipse.che.api.machine.server.spi.Instance;
import org.eclipse.che.api.machine.server.spi.SnapshotDao;
import org.eclipse.che.api.workspace.server.model.impl.EnvironmentImpl;
import org.eclipse.che.api.workspace.server.model.impl.WorkspaceRuntimeImpl;
import org.eclipse.che.api.workspace.shared.dto.event.WorkspaceStatusEvent;
import org.eclipse.che.api.workspace.shared.dto.event.WorkspaceStatusEvent.EventType;
import org.eclipse.che.commons.lang.concurrent.CloseableLock;
import org.eclipse.che.commons.lang.concurrent.StripedLocks;
import org.eclipse.che.dto.server.DtoFactory;
import org.slf4j.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.RUNNING;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.SNAPSHOTTING;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.STOPPING;
import static org.eclipse.che.api.machine.shared.Constants.ENVIRONMENT_OUTPUT_CHANNEL_TEMPLATE;
import static org.slf4j.LoggerFactory.getLogger;

// TODO implement logic when thread is cancelled before an executor submitted it

/**
 * Defines an internal API for managing {@link WorkspaceRuntimeImpl} instances.
 *
 * <p>This component implements {@link WorkspaceStatus} contract.
 *
 * <p>The implementation is thread-safe and guarded by
 * eagerly initialized readwrite locks produced by {@link StripedLocks}.
 * The component doesn't expose any api for client-side locking.
 * All the instances produced by this component are copies of the real data.
 *
 * <p>The component doesn't check if the incoming objects are in application-valid state.
 * Which means that it is expected that if {@link #start(Workspace, String, boolean)} method is called
 * then {@code WorkspaceImpl} argument is a application-valid object which contains
 * all the required data for performing start.
 *
 * @author Yevhenii Voevodin
 * @author Alexander Garagatyi
 */
@Singleton
public class WorkspaceRuntimes {

    private static final Logger LOG = getLogger(WorkspaceRuntimes.class);

    private final Map<String, WorkspaceState> states;
    private final EventService                eventsService;
    private final StripedLocks                locks;
    private final CheEnvironmentEngine        envEngine;
    private final AgentSorter                 agentSorter;
    private final AgentLauncherFactory        launcherFactory;
    private final AgentRegistry               agentRegistry;
    private final SnapshotDao                 snapshotDao;
    private final WorkspaceSharedPool         sharedPool;

    private volatile boolean isPreDestroyInvoked;

    @Inject
    public WorkspaceRuntimes(EventService eventsService,
                             CheEnvironmentEngine envEngine,
                             AgentSorter agentSorter,
                             AgentLauncherFactory launcherFactory,
                             AgentRegistry agentRegistry,
                             SnapshotDao snapshotDao,
                             WorkspaceSharedPool sharedPool) {
        this.eventsService = eventsService;
        this.envEngine = envEngine;
        this.agentSorter = agentSorter;
        this.launcherFactory = launcherFactory;
        this.agentRegistry = agentRegistry;
        this.snapshotDao = snapshotDao;
        this.states = new HashMap<>();
        // 16 - experimental value for stripes count, it comes from default hash map size
        this.locks = new StripedLocks(16);
        this.sharedPool = sharedPool;
    }

    /**
     * Returns the runtime descriptor describing workspace runtime.
     *
     * <p>Note that the {@link RuntimeDescriptor#getRuntime()} method
     * returns a copy of a real {@code WorkspaceRuntime} object,
     * which means that any runtime copy modifications won't affect the
     * real object and also it means that copy won't be affected with modifications applied
     * to the real runtime workspace object state.
     *
     * @param workspaceId
     *         the id of the workspace to get its runtime
     * @return descriptor which describes current state of the workspace runtime
     * @throws NotFoundException
     *         when workspace with given {@code workspaceId} is not found
     * @throws ServerException
     *         if environment is in illegal state
     */
    public RuntimeDescriptor get(String workspaceId) throws NotFoundException, ServerException {
        WorkspaceState state;
        try (@SuppressWarnings("unused") CloseableLock l = locks.readLock(workspaceId)) {
            state = new WorkspaceState(getExistingState(workspaceId));
        }
        WorkspaceRuntimeImpl runtime = new WorkspaceRuntimeImpl(state.activeEnv, envEngine.getMachines(workspaceId));
        return new RuntimeDescriptor(state.status, runtime);
    }

    /**
     * Returns true if the workspace with given id has
     * any status different from {@link WorkspaceStatus#STOPPED}.
     *
     * @param workspaceId
     *         workspace identifier to perform check
     * @return true if workspace status is different from {@link WorkspaceStatus#STOPPED}
     */
    public boolean hasRuntime(String workspaceId) {
        try (@SuppressWarnings("unused") CloseableLock l = locks.readLock(workspaceId)) {
            return states.containsKey(workspaceId);
        }
    }

    /**
     * Starts all machines from specified workspace environment,
     * creates workspace runtime instance based on that environment.
     *
     * <p>During the start of the workspace its
     * runtime is visible with {@link WorkspaceStatus#STARTING} status.
     *
     * @param workspace
     *         workspace which environment should be started
     * @param envName
     *         the name of the environment to start
     * @param recover
     *         whether machines should be recovered(true) or not(false)
     * @return the workspace runtime instance with machines set.
     * @throws ConflictException
     *         when workspace is already running
     * @throws NotFoundException
     *         when any not found exception occurs during environment start
     * @throws ServerException
     *         other error occurs during environment start
     * @see CheEnvironmentEngine#start(String, String, Environment, boolean, MessageConsumer)
     * @see WorkspaceStatus#STARTING
     * @see WorkspaceStatus#RUNNING
     */
    public RuntimeDescriptor start(Workspace workspace,
                                   String envName,
                                   boolean recover) throws ServerException,
                                                           ConflictException,
                                                           NotFoundException {
        Future<RuntimeDescriptor> future = startAsync(workspace, envName, recover);
        try {
            return future.get();
        } catch (InterruptedException x) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new ServerException(x.getMessage(), x);
        } catch (ExecutionException execEx) {
            try {
                throw execEx.getCause();
            } catch (ServerException x) {
                throw x;
            } catch (Throwable thr) {
                throw new ServerException(thr.getMessage(), thr);
            }
        }
    }

    /**
     * Starts the workspace like {@link #start(Workspace, String, boolean)}
     * method does, but asynchronously. Nonetheless synchronously checks that workspace
     * doesn't have runtime and makes it {@link WorkspaceStatus#STARTING}.
     *
     * <p>It's allowed to stop the {@link WorkspaceStatus#RUNNING} or
     * {@link WorkspaceStatus#STARTING} workspace either by calling {@link #stop(String)}
     * or by cancelling the returned future with interruption e.g. {@code future.cancel(true)}.
     */
    public Future<RuntimeDescriptor> startAsync(Workspace workspace,
                                                String envName,
                                                boolean recover) throws ConflictException {
        EnvironmentImpl environment = copyEnv(workspace, envName);
        String workspaceId = workspace.getId();
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            WorkspaceState state = states.get(workspaceId);
            if (state != null) {
                throw new ConflictException(format("Could not start workspace '%s' because its status is '%s'",
                                                   workspace.getConfig().getName(),
                                                   state.status));
            }
            states.put(workspaceId, state = new WorkspaceState(WorkspaceStatus.STARTING, envName));
            return state.startingTask = sharedPool.submit(() -> {
                startEnvironmentAndPublishEvents(environment, workspaceId, envName, recover);
                return get(workspaceId);
            });
        }
    }

    /**
     * Stops running workspace runtime.
     *
     * <p>Stops environment in an implementation specific way.
     * During the stop of the workspace its runtime is accessible with {@link WorkspaceStatus#STOPPING stopping} status.
     * Workspace may be stopped only if its status is {@link WorkspaceStatus#RUNNING}.
     *
     * @param workspaceId
     *         identifier of workspace which should be stopped
     * @throws NotFoundException
     *         when workspace with specified identifier is not running
     * @throws ServerException
     *         when any error occurs during workspace stopping
     * @throws ConflictException
     *         when running workspace status is different from {@link WorkspaceStatus#RUNNING}
     * @see CheEnvironmentEngine#stop(String)
     * @see WorkspaceStatus#STOPPING
     */
    public void stop(String workspaceId) throws NotFoundException, ServerException, ConflictException {
        Future<?> startingTask;
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            WorkspaceState state = getExistingState(workspaceId);
            switch (state.status) {
                case RUNNING:
                    state.status = WorkspaceStatus.STOPPING;
                    break;
                case STARTING:
                    state.startingTask.cancel(true);
                    break;
                default:
                    throw new ConflictException(format("Couldn't stop '%s' workspace because its status is '%s'. " +
                                                       "Workspace can be stopped only if it is 'RUNNING' or 'STARTING'",
                                                       workspaceId,
                                                       state.status));
            }
            startingTask = state.startingTask;
        }

        // workspace is running, stop normally
        if (startingTask == null) {
            stopEnvironmentAndPublishEvents(workspaceId, WorkspaceStatus.RUNNING, false);
            return;
        }

        // waiting for starting task to stop the workspace and cleanup the resources
        try {
            startingTask.get();
        } catch (InterruptedException x) {
            Thread.currentThread().interrupt();
            throw new ServerException(x.getMessage(), x);
        } catch (ExecutionException execEx) {
            try {
                throw execEx.getCause();
            } catch (NotFoundException x) {
                throw x;
            } catch (Throwable thr) {
                throw new ServerException(thr.getMessage(), thr);
            }
        }
    }

    /**
     * Starts machine in running workspace.
     *
     * @param workspaceId
     *         ID of workspace that owns machine
     * @param machineConfig
     *         config of machine that should be started
     * @return running machine
     * @throws ConflictException
     *         if environment is not running or conflicting machine already exists in the environment
     * @throws ConflictException
     *         if environment was stopped during start of machine
     * @throws ServerException
     *         if any other error occurs
     */
    public Instance startMachine(String workspaceId,
                                 MachineConfig machineConfig) throws ServerException,
                                                                     ConflictException,
                                                                     NotFoundException,
                                                                     EnvironmentException {

        try (CloseableLock lock = locks.readLock(workspaceId)) {
            getRunningState(workspaceId);
        }

        // Copy constructor makes deep copy of objects graph
        // which means that original values won't affect the values in used further in this class
        MachineConfigImpl machineConfigCopy = new MachineConfigImpl(machineConfig);

        List<String> agents = Collections.singletonList("org.eclipse.che.terminal");

        Instance instance = envEngine.startMachine(workspaceId, machineConfigCopy, agents);
        launchAgents(instance, agents);

        try (CloseableLock lock = locks.writeLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            WorkspaceState workspaceState = states.get(workspaceId);
            if (workspaceState == null || workspaceState.status != RUNNING) {
                try {
                    envEngine.stopMachine(workspaceId, instance.getId());
                } catch (NotFoundException | ServerException | ConflictException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
                throw new ConflictException(format("Environment of workspace '%s' was stopped during start of  machine",
                                                   workspaceId));
            }
        }
        return instance;
    }

    /**
     * Synchronously creates a snapshot of a given workspace,
     * the workspace must be {@link WorkspaceStatus#RUNNING}.
     *
     * <p>Publishes {@link EventType#SNAPSHOT_CREATING}, {@link EventType#SNAPSHOT_CREATED},
     * {@link EventType#SNAPSHOT_CREATION_ERROR} like defined by {@link EventType}.
     *
     * @param workspaceId
     *         the id of workspace to create snapshot
     * @throws NotFoundException
     *         when workspace doesn't have a runtime
     * @throws ConflictException
     *         when workspace status is different from {@link WorkspaceStatus#RUNNING}
     * @throws ServerException
     *         when any other error occurs
     */
    public void snapshot(String workspaceId) throws NotFoundException,
                                                    ConflictException,
                                                    ServerException {
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(workspaceId)) {
            getRunningState(workspaceId).status = SNAPSHOTTING;
        }
        snapshotAndUpdateStatus(workspaceId);
    }

    /**
     * Asynchronously creates a snapshot of a given workspace,
     * but synchronously toggles workspace status to {@link WorkspaceStatus#SNAPSHOTTING}
     * or throws an error if it is impossible to do so.
     *
     * @see #snapshot(String)
     */
    public Future<Void> snapshotAsync(String workspaceId) throws NotFoundException, ConflictException {
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(workspaceId)) {
            getRunningState(workspaceId).status = SNAPSHOTTING;
        }
        return sharedPool.submit(() -> {
            try {
                snapshotAndUpdateStatus(workspaceId);
            } catch (Exception x) {
                LOG.error(format("Couldn't create a snapshot of workspace '%s'", workspaceId), x);
                throw x;
            }
            return null;
        });
    }

    /**
     * Removes snapshot binaries in implementation specific way.
     *
     * @param snapshot
     *         snapshot that will be removed
     * @return true if binaries are successfully removed,
     * otherwise if binaries not found returns false
     * @throws ServerException
     *         if any error occurs during binaries removal
     * @see CheEnvironmentEngine#removeSnapshot(SnapshotImpl)
     */
    public boolean removeBinaries(SnapshotImpl snapshot) throws ServerException {
        try {
            envEngine.removeSnapshot(snapshot);
        } catch (NotFoundException x) {
            return false;
        }
        return true;
    }

    /**
     * Removes binaries of all the snapshots, continues to remove
     * snapshots if removal of binaries for a single snapshot fails.
     *
     * @param snapshots
     *         the list of snapshots to remove binaries
     */
    public void removeBinaries(Collection<? extends SnapshotImpl> snapshots) {
        for (SnapshotImpl snapshot : snapshots) {
            try {
                if (!removeBinaries(snapshot)) {
                    LOG.warn("An attempt to remove binaries of the snapshot '{}' while there are no binaries", snapshot.getId());
                }
            } catch (ServerException x) {
                LOG.error(format("Couldn't remove snapshot '%s', workspace id '%s'", snapshot.getId(), snapshot.getWorkspaceId()), x);
            }
        }
    }

    /**
     * Stops machine in a running environment.
     *
     * @param workspaceId
     *         ID of workspace that owns environment
     * @param machineId
     *         ID of machine that should be stopped
     * @throws NotFoundException
     *         if machine is not found in the environment
     *         or workspace doesn't have a runtime
     * @throws ConflictException
     *         if environment is not running
     * @throws ConflictException
     *         if machine is dev and its stop is forbidden
     * @throws ServerException
     *         if any other error occurs
     */
    public void stopMachine(String workspaceId, String machineId) throws NotFoundException,
                                                                         ServerException,
                                                                         ConflictException {
        try (CloseableLock lock = locks.readLock(workspaceId)) {
            getRunningState(workspaceId);
        }
        envEngine.stopMachine(workspaceId, machineId);
    }

    /**
     * Finds machine {@link Instance} by specified workspace and machine IDs.
     *
     * @param workspaceId
     *         ID of workspace that owns machine
     * @param machineId
     *         ID of requested machine
     * @return requested machine
     * @throws NotFoundException
     *         if environment or machine is not running
     */
    public Instance getMachine(String workspaceId, String machineId) throws NotFoundException {
        return envEngine.getMachine(workspaceId, machineId);
    }

    /**
     * Returns all states with statuses of its active environment.
     */
    public Map<String, WorkspaceState> getWorkspaces() {
        return new HashMap<>(states);
    }

    /**
     * Removes all states from the in-memory storage, while
     * {@link CheEnvironmentEngine} is responsible for environment destroying.
     */
    @PreDestroy
    @VisibleForTesting
    void cleanup() {
        isPreDestroyInvoked = true;

        // wait existing tasks to complete
        sharedPool.terminateAndWait();

        List<String> idsToStop;
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeAllLock()) {
            idsToStop = states.entrySet()
                              .stream()
                              .filter(e -> e.getValue().status != STOPPING)
                              .map(Map.Entry::getKey)
                              .collect(Collectors.toList());
            states.clear();
        }

        // nothing to stop
        if (idsToStop.isEmpty()) {
            return;
        }

        LOG.info("Shutdown running states, states to shutdown '{}'", idsToStop.size());
        ExecutorService executor =
                Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
                                             new ThreadFactoryBuilder().setNameFormat("StopEnvironmentsPool-%d")
                                                                       .setDaemon(false)
                                                                       .build());
        for (String id : idsToStop) {
            executor.execute(() -> {
                try {
                    envEngine.stop(id);
                } catch (Exception x) {
                    LOG.error(x.getMessage(), x);
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.error("Unable terminate machines pool");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void ensurePreDestroyIsNotExecuted() {
        if (isPreDestroyInvoked) {
            throw new IllegalStateException("Could not perform operation because application server is stopping");
        }
    }

    private WorkspaceState getExistingState(String workspaceId) throws NotFoundException {
        WorkspaceState state = states.get(workspaceId);
        if (state == null) {
            throw new NotFoundException("Workspace with id '" + workspaceId + "' is not running");
        }
        return state;
    }

    private WorkspaceState getRunningState(String workspaceId) throws NotFoundException, ConflictException {
        WorkspaceState state = getExistingState(workspaceId);
        if (state.status != RUNNING) {
            throw new ConflictException(format("Workspace with id '%s' is not 'RUNNING', it's status is '%s'",
                                               workspaceId,
                                               state.status));
        }
        return state;
    }

    protected void launchAgents(Instance instance, List<String> agents) throws ServerException {
        try {
            for (AgentKey agentKey : agentSorter.sort(agents)) {
                if (!Thread.currentThread().isInterrupted()) {
                    LOG.info("Launching '{}' agent at workspace {}", agentKey.getId(), instance.getWorkspaceId());
                    Agent agent = agentRegistry.getAgent(agentKey);
                    AgentLauncher launcher = launcherFactory.find(agentKey.getId(), instance.getConfig().getType());
                    launcher.launch(instance, agent);
                }
            }
        } catch (AgentException e) {
            throw new MachineException(e.getMessage(), e);
        }
    }

    /**
     * Starts the environment publishing all the necessary events.
     * Respects task interruption & stops the workspace if starting task is cancelled.
     */
    private void startEnvironmentAndPublishEvents(EnvironmentImpl environment,
                                                  String workspaceId,
                                                  String envName,
                                                  boolean recover) throws ServerException, EnvironmentException, ConflictException {
        eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                        .withWorkspaceId(workspaceId)
                                        .withStatus(WorkspaceStatus.STARTING)
                                        .withEventType(EventType.STARTING)
                                        .withPrevStatus(WorkspaceStatus.STOPPED));
        try {
            envEngine.start(workspaceId,
                            envName,
                            environment,
                            recover,
                            new WebsocketMessageConsumer<>(format(ENVIRONMENT_OUTPUT_CHANNEL_TEMPLATE, workspaceId)),
                            new MachineAgentsLauncher(environment.getMachines()));
        } catch (EnvironmentStartInterruptedException x) {
            // environment start was cancelled, finish normally STARING -> ... -> STOPPED
            compareAndSetStatus(workspaceId, WorkspaceStatus.STARTING, WorkspaceStatus.STOPPING);
            stopEnvironmentAndPublishEvents(workspaceId, WorkspaceStatus.STARTING, true);
            return;
        } catch (EnvironmentException | ServerException | ConflictException x) {
            // environment can't be stopped for some reason, STARTING -> STOPPED
            removeState(workspaceId);
            eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                            .withWorkspaceId(workspaceId)
                                            .withEventType(EventType.ERROR)
                                            .withPrevStatus(WorkspaceStatus.STARTING)
                                            .withError("Start of environment " + envName + " failed. Error: " + x.getMessage()));
            throw x;
        }

        // disallow the workspace start cancellation, STARTING -> RUNNING
        Future<?> startingTask;
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(workspaceId)) {
            WorkspaceState state = states.get(workspaceId);
            startingTask = state.startingTask;
            if (!startingTask.isCancelled()) {
                state.status = WorkspaceStatus.RUNNING;
            }
            state.startingTask = null;
        }

        if (startingTask.isCancelled()) {
            // workspace start canceled before workspace is fully started, STARTING -> ... -> STOPPED
            stopEnvironmentAndPublishEvents(workspaceId, WorkspaceStatus.STARTING, false);
        } else {
            // normally started, notify clients
            eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                            .withWorkspaceId(workspaceId)
                                            .withStatus(WorkspaceStatus.RUNNING)
                                            .withEventType(EventType.RUNNING)
                                            .withPrevStatus(WorkspaceStatus.STARTING));
        }
    }

    /**
     * Stops the workspace publishing all the necessary events.
     * If {@code ignoreNotFound} is true & environment for given workspace
     * doesn't exist then stop finishes successfully otherwise stop fails
     * with {@link ServerException} rethrowing any occurred error.
     */
    private void stopEnvironmentAndPublishEvents(String workspaceId,
                                                 WorkspaceStatus prevStatus,
                                                 boolean ignoreNotFound) throws ServerException {
        eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                        .withWorkspaceId(workspaceId)
                                        .withPrevStatus(prevStatus)
                                        .withStatus(WorkspaceStatus.STOPPING)
                                        .withEventType(EventType.STOPPING));
        removeState(workspaceId);

        // stops the environment and either rethrows not found exception
        // as server exception or suppresses and finishes normally
        try {
            envEngine.stop(workspaceId);
        } catch (Exception x) {
            if (!(x instanceof EnvironmentNotRunningException) || !ignoreNotFound) {
                eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                                .withWorkspaceId(workspaceId)
                                                .withPrevStatus(prevStatus)
                                                .withEventType(EventType.ERROR)
                                                .withError(x.getMessage())
                                                .withStatus(WorkspaceStatus.STOPPED));
                throw new ServerException(x.getMessage(), x);
            }
        }

        eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                        .withWorkspaceId(workspaceId)
                                        .withPrevStatus(prevStatus)
                                        .withEventType(EventType.STOPPED)
                                        .withStatus(WorkspaceStatus.STOPPED));
    }

    private static EnvironmentImpl copyEnv(Workspace workspace, String envName) {
        final Environment environment = workspace.getConfig().getEnvironments().get(envName);
        if (environment == null) {
            throw new IllegalArgumentException(format("Workspace '%s' doesn't contain environment '%s'",
                                                      workspace.getId(),
                                                      envName));
        }
        return new EnvironmentImpl(environment);
    }

    /**
     * Safely compares current status of given workspace
     * with {@code from} and if they are equal sets the status to {@code to}.
     * Returns true if the status of workspace was updated with {@code to} value.
     */
    private boolean compareAndSetStatus(String id, WorkspaceStatus from, WorkspaceStatus to) {
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(id)) {
            ensurePreDestroyIsNotExecuted();
            WorkspaceState state = states.get(id);
            if (state != null && state.status == from) {
                state.status = to;
                return true;
            }
        }
        return false;
    }

    /** Removes state from in-memory storage in write lock. */
    private void removeState(String workspaceId) {
        try (@SuppressWarnings("unused") CloseableLock l = locks.writeLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            states.remove(workspaceId);
        }
    }

    /** Creates a snapshot and changes status SNAPSHOTTING -> RUNNING. */
    private void snapshotAndUpdateStatus(String workspaceId) throws NotFoundException,
                                                                    ConflictException,
                                                                    ServerException {
        eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                        .withWorkspaceId(workspaceId)
                                        .withStatus(WorkspaceStatus.SNAPSHOTTING)
                                        .withEventType(EventType.SNAPSHOT_CREATING)
                                        .withPrevStatus(WorkspaceStatus.RUNNING));

        WorkspaceRuntimeImpl runtime = get(workspaceId).getRuntime();
        List<MachineImpl> machines = runtime.getMachines();
        machines.sort(comparing(m -> !m.getConfig().isDev(), Boolean::compare));

        LOG.info("Creating snapshot of workspace '{}', machines to snapshot: '{}'", workspaceId, machines.size());
        List<SnapshotImpl> newSnapshots = new ArrayList<>(machines.size());
        for (MachineImpl machine : machines) {
            try {
                newSnapshots.add(envEngine.saveSnapshot(workspaceId, machine.getId()));
            } catch (ServerException | NotFoundException x) {
                if (machine.getConfig().isDev()) {
                    compareAndSetStatus(workspaceId, WorkspaceStatus.SNAPSHOTTING, WorkspaceStatus.RUNNING);
                    eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                                    .withWorkspaceId(workspaceId)
                                                    .withStatus(WorkspaceStatus.RUNNING)
                                                    .withEventType(EventType.SNAPSHOT_CREATION_ERROR)
                                                    .withPrevStatus(WorkspaceStatus.SNAPSHOTTING)
                                                    .withError(x.getMessage()));
                    throw x;
                }
                LOG.warn(format("Couldn't create snapshot of machine '%s:%s' in workspace '%s'",
                                machine.getEnvName(),
                                machine.getConfig().getName(),
                                workspaceId));
            }
        }

        LOG.info("Saving new snapshots metadata, workspace id '{}'", workspaceId);
        try {
            List<SnapshotImpl> removed = snapshotDao.replaceSnapshots(workspaceId,
                                                                      runtime.getActiveEnv(),
                                                                      newSnapshots);
            if (!removed.isEmpty()) {
                LOG.info("Removing old snapshots binaries, workspace id '{}', snapshots to remove '{}'", workspaceId, removed.size());
                removeBinaries(removed);
            }
        } catch (SnapshotException x) {
            LOG.error(format("Couldn't remove existing snapshots metadata for workspace '%s'", workspaceId), x);
            LOG.info("Removing newly created snapshots, workspace id '{}', snapshots to remove '{}'", workspaceId, newSnapshots.size());
            removeBinaries(newSnapshots);
            compareAndSetStatus(workspaceId, WorkspaceStatus.SNAPSHOTTING, WorkspaceStatus.RUNNING);
            eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                            .withWorkspaceId(workspaceId)
                                            .withStatus(WorkspaceStatus.RUNNING)
                                            .withEventType(EventType.SNAPSHOT_CREATION_ERROR)
                                            .withPrevStatus(WorkspaceStatus.SNAPSHOTTING)
                                            .withError(x.getMessage()));
            throw x;
        }
        compareAndSetStatus(workspaceId, WorkspaceStatus.SNAPSHOTTING, WorkspaceStatus.RUNNING);
        eventsService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                        .withStatus(WorkspaceStatus.RUNNING)
                                        .withWorkspaceId(workspaceId)
                                        .withEventType(EventType.SNAPSHOT_CREATED)
                                        .withPrevStatus(WorkspaceStatus.SNAPSHOTTING));
    }

    public static class WorkspaceState {
        private WorkspaceStatus           status;
        private String                    activeEnv;
        private Future<RuntimeDescriptor> startingTask;

        private WorkspaceState(WorkspaceState state) {
            this.status = state.status;
            this.activeEnv = state.activeEnv;
            this.startingTask = state.startingTask;
        }

        public WorkspaceState(WorkspaceStatus status, String activeEnv) {
            this.status = status;
            this.activeEnv = activeEnv;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WorkspaceState)) return false;
            WorkspaceState that = (WorkspaceState)o;
            return status == that.status &&
                   Objects.equals(activeEnv, that.activeEnv);
        }

        @Override
        public int hashCode() {
            return Objects.hash(status, activeEnv);
        }
    }

    /**
     * Wrapper for the {@link WorkspaceRuntime} instance.
     * Knows the state of the started workspace runtime,
     * helps to postpone {@code WorkspaceRuntime} instance creation to
     * the time when all the machines from the workspace are created.
     */
    public static class RuntimeDescriptor {

        private WorkspaceRuntimeImpl runtime;
        private WorkspaceStatus      status;

        public RuntimeDescriptor(WorkspaceStatus workspaceStatus, WorkspaceRuntimeImpl runtime) {
            this.status = workspaceStatus;
            this.runtime = runtime;
        }

        /** Returns the instance of {@code WorkspaceRuntime} described by this descriptor. */
        public WorkspaceRuntimeImpl getRuntime() {
            return runtime;
        }

        public void setRuntime(WorkspaceRuntimeImpl runtime) {
            this.runtime = runtime;
        }

        /**
         * Returns the status of the {@code WorkspaceRuntime} described by this descriptor.
         * Never returns {@link WorkspaceStatus#STOPPED} status, you'll rather get {@link NotFoundException}
         * from {@link #get(String)} method.
         */
        public WorkspaceStatus getRuntimeStatus() {
            return status;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RuntimeDescriptor)) return false;
            RuntimeDescriptor that = (RuntimeDescriptor)o;
            return Objects.equals(runtime, that.runtime) &&
                   status == that.status;
        }

        @Override
        public int hashCode() {
            return Objects.hash(runtime, status);
        }

        @Override
        public String toString() {
            return "RuntimeDescriptor{" +
                   "runtime=" + runtime +
                   ", status=" + status +
                   '}';
        }
    }

    private class MachineAgentsLauncher implements MachineStartedHandler {

        private final Map<String, ? extends ExtendedMachine> nameToMachine;

        private MachineAgentsLauncher(Map<String, ? extends ExtendedMachine> nameToMachine) {
            this.nameToMachine = nameToMachine;
        }

        @Override
        public void started(Instance machine) throws ServerException {
            launchAgents(machine, nameToMachine.get(machine.getConfig().getName()).getAgents());
        }
    }
}
