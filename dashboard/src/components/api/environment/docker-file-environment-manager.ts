/*
 * Copyright (c) 2015-2016 Codenvy, S.A.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Codenvy, S.A. - initial API and implementation
 */
'use strict';

import {EnvironmentManager} from './environment-manager';
import {DockerfileParser} from './docker-file-parser';
import {EnvironmentManagerMachine, IEnvironmentManagerMachine} from './environment-manager-machine';

/**
 * This is the implementation of environment manager that handles the docker file format of environment.
 *
 * Format sample and specific description:
 * <code>
 * FROM ubuntu
 * RUN mkdir /var/www
 * ADD app.js /var/www/app.js
 * CMD ["/usr/bin/node", "/var/www/app.js"]
 * </code>
 *
 * The recipe type is <code>dockerfile</code>. This environment can contain only one machine.
 * Machine is described both in recipe (content or location to recipe) and in machines attribute of the environment (machine configs).
 * The machine configs contain memoryLimitBytes in attributes, servers and agent.
 * Environment variables can be set only in recipe content.
 *
 * @author Ann Shumilova
 */

const ENV_INSTRUCTION: string = 'ENV';

export class DockerFileEnvironmentManager extends EnvironmentManager {
  $log: ng.ILogService;
  parser: DockerfileParser;

  constructor($log: ng.ILogService) {
    super();

    this.$log = $log;

    this.parser = new DockerfileParser();
  }

  get editorMode(): string {
    return 'text/x-dockerfile';
  }

  /**
   * Parses a dockerfile and returns an array of objects
   *
   * @param content {string} content of dockerfile
   * @returns {Array} a list of instructions and arguments
   * @private
   */
  _parseRecipe(content: string): any[] {
    let recipe: any[] = null;
    try {
      recipe = this.parser.parse(content);
    } catch (e) {
      this.$log.error(e);
    }
    return recipe;
  }

  /**
   * Dumps a list of instructions and arguments into dockerfile
   *
   * @param instructions {Array} array of objects
   * @returns {string} dockerfile
   * @private
   */
  _stringifyRecipe(instructions: any[]): string {
    let content = '';

    try {
      content = this.parser.dump(instructions);
    } catch (e) {
      this.$log.log(e);
    }

    return content;
  }

  /**
   * Provides the environment configuration based on machines format.
   *
   * @param {che.IWorkspaceEnvironment} environment origin of the environment to be edited
   * @param {IEnvironmentManagerMachine[]} machines the list of machines
   * @returns {che.IWorkspaceEnvironment} environment's configuration
   */
  getEnvironment(environment: che.IWorkspaceEnvironment, machines: IEnvironmentManagerMachine[]): che.IWorkspaceEnvironment {
    let newEnvironment = super.getEnvironment(environment, machines);

    // machines should contain one machine only
    if (machines[0].recipe) {
      try {
        newEnvironment.recipe.content = this._stringifyRecipe(machines[0].recipe);
      } catch (e) {
        this.$log.error('Cannot retrieve environment\'s recipe, error: ', e);
      }
    }

    return newEnvironment;
  }

  /**
   * Retrieves the list of machines.
   *
   * @param {che.IWorkspaceEnvironment} environment environment's configuration
   * @returns {IEnvironmentManagerMachine[]} list of machines defined in environment
   */
  getMachines(environment: che.IWorkspaceEnvironment): IEnvironmentManagerMachine[] {
    let recipe = null,
        machines: IEnvironmentManagerMachine[] = [];

    if (environment.recipe.content) {
      recipe = this._parseRecipe(environment.recipe.content);
    }

    Object.keys(environment.machines).forEach((machineName: string) => {
      let envMachine = angular.copy(environment.machines[machineName]),
          machine = new EnvironmentManagerMachine(machineName, recipe);
      if (envMachine.attributes) {
        machine.attributes = angular.copy(envMachine.attributes);
      }
      if (envMachine.agents) {
        machine.agents = angular.copy(envMachine.agents);
      }
      if (envMachine.servers) {
        machine.servers = angular.copy(envMachine.servers);
      }

      machines.push(machine);
    });

    return machines;
  }

  /**
   * Returns a docker image from the recipe.
   *
   * @param {IEnvironmentManagerMachine} machine
   * @returns {*}
   */
  getSource(machine: IEnvironmentManagerMachine): {image: string} {
    if (!machine.recipe) {
      return null;
    }

    let from = machine.recipe.find((line: any) => {
      return line.instruction === 'FROM';
    });

    return {image: from.argument};
  }

  /**
   * Returns true if environment recipe content is present.
   *
   * @param {IEnvironmentManagerMachine} machine
   * @returns {boolean}
   */
  canEditEnvVariables(machine: IEnvironmentManagerMachine): boolean {
    return !!machine.recipe;
  }

  /**
   * Returns environment variables from recipe
   *
   * @param {IEnvironmentManagerMachine} machine
   * @returns {*}
   */
  getEnvVariables(machine: IEnvironmentManagerMachine): any {
    if (!machine.recipe) {
      return null;
    }

    let envVariables = {};

    let envList = machine.recipe.filter((line: any) => {
      return line.instruction === ENV_INSTRUCTION;
    }) || [];

    envList.forEach((line: any) => {
      envVariables[line.argument[0]] = line.argument[1];
    });

    return envVariables;
  }

  /**
   * Updates machine with new environment variables.
   *
   * @param {IEnvironmentManagerMachine} machine
   * @param {*} envVariables
   */
  setEnvVariables(machine: IEnvironmentManagerMachine, envVariables: any): void {
    if (!machine.recipe) {
      return;
    }

    let newRecipe = [];

    // new recipe without any 'ENV' instruction
    newRecipe = machine.recipe.filter((line: any) => {
      return line.instruction !== ENV_INSTRUCTION;
    });

    // add environments if any
    if (Object.keys(envVariables).length) {
      Object.keys(envVariables).forEach((name: string) => {
        let line = {
          instruction: ENV_INSTRUCTION,
          argument: [name, envVariables[name]]
        };
        newRecipe.splice(1, 0, line);
      });
    }

    machine.recipe = newRecipe;
  }
}
