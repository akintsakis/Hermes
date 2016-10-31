# Hermes

# Hermes - Containerized high performance bioinformatics workflow execution

##Dependencies

* Supported operating systems Linux distributions, however Hermes has only been tested in Ubuntu Linux 14.04 and 16.04

* Please ensure that the latest version of docker is installed on all sites you plan to execute on and that your user is in the list of users that can access the docker daemon. To install docker visit: https://docs.docker.com/linux/step_one/

* SSH server must be up and running on all sites you plan to execute on and password-less access via SSH keys must be enabled. All sites must be accessible through the same SSH key which must NOT be protected by a password (passphrase). To setup password-less SSH access follow this guide: http://www.tecmint.com/ssh-passwordless-login-using-ssh-keygen-in-5-easy-steps/

* You must also install Java Oracle version 8. 

## Step 1 - Configuration
Configuring Hermes is easy and straightforward. Open the configuration.config file in the root hermes projectfolder, the only parameter that you need to edit is the pathToSSHKEy.

* Provide a value to the pathToSSHKEy parameter

Here you must provide the path to the SSH Private key that will be used to connect to the local as well as all remote sites. You must configure all sites, including the one running on the same machine that Hermes is running, to be accessible through this SSH key. Please read the warning regarding the SSH key within the configuration file. All other options do not need to be edited from their default values unless you are experiencing issues.

Please note, if external sites (other than local) are utilized, the SSH port 22 must be open and the SSH server listening, on the machine here Hermes is running.

## Step 2 - Configuring execution sites
Navigate within the /Sites/ folder located in the root hermes project folder. You can add execution sites by placing .site files within this folder. The default, local.site file already exists for your reference. Within the .site file state a name for the site, for the hostname (IP address) of the site, please provide the global external IP address, or you can provide localhost in case the site is the one Hermes is running on.

* provide a value for name
* provide a value for username
* provide a value for ipAddress (or hostname), or localhost if local

You must also the ssh listening port on the site (default value is 22).

You can add as many sites as you wish by creating *.site files within this folder and hermes will automatically utilize them and load balance the work between them. You do not need to have a local site, you can utilize remote sites exclusively if you wish.


## Step 3 - Running the pangenome analysis sample workflow with the provided sample input
To execute the pangenome analysis sample workflow you must navigate via a terminal within the hermes project root folder and execute the command

* java -jar ./Hermes/dist/Hermes.jar

Hermes automatically runs the pangenome analysis sample workflow when no command line arguments are provided. The results of the workflow can be found in a folder in your home folder named hermes_workflow_results followed by the current date.

In case you want to run a different workflow, the first provided argument points to the .graphml workflow description file, while the second argument is the ABSOLUTE path to the workflow inputs folder.

For example, in order to run the also provided phylogenetic profiles workflow, the command would be:

* java -jar ./Hermes/dist/Hermes.jar pathToWorkflowFile pathToInputsFolder

The paths must be absolute, not relative. A sample command would be

* java -jar ./Hermes/dist/Hermes.jar /home/user/hermes/WorkflowGraphs/phylogenetic_profiles.graphml /home/user/hermes/WorkflowSampleInputs/SmallSampleInput/

This command will run the phylogenetic profiles workflow (assuming that the paths are valid for your file system) by using the provided SmallSampleInput dataset.

In case you want to run a larger analysis, you can use the input dataset located in the WorkflowSampleInputs/ExtendedSampleInput folder.

## Common errors

Hermes is a distributed computing platform and as such network errors may sometimes impede optimal execution. In case you encounter any such error, please re-run the workflow.

* While an image is being pulled from dockerhub, the connection is reset. In this case you need to cancel (ctrl-c) and re-run the workflow.
