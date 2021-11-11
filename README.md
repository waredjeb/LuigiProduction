## Trigger Scale Factor Studies

Requirements:

- ```python 3.9``` (likely works on other Python 3 versions)
- ```luigi``` (available in ```CMSSW``` after running ```cmsenv``` and using ```python3```).


## Tasks (steps of the study)

- Running HTCondor jobs

```shell
#Example
python3 /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/METTriggerStudies/scripts/submit_triggerEff.py
        --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/
	--outdir /data_CMS/cms/alves/TriggerScaleFactors
	--tag v1
	--proc MET2018A MET2018B MET2018C MET2018D
	--channels all etau mutau tautau mumu
```

Output ```ROOT``` files: ```/data_CMS/cms/alves/TriggerScaleFactors/v1/```
Submission files: ```$HOME/jobs/submission/```
Job standard output and error streams: ```$HOME/jobs/outputs/```


Check the meaning of the arguments by adding ```--help```.

- Running the ```luigi``` workflow

TODO


-------------------------------------

### Luigi Workflow

The advantages of using a workflow management system as ```luigi``` are the following:

- the whole chain can be run at once
- the workflow is clearer from a code point of view
- the configuration is accessible from within a single file (```luigi.cfg```)
- a task in the chain is run only if at least one output file of a task on which it depends is more recent that at least one of the output files it already produced (via the ```ForceableEnsureRecentTarget``` custom class in ```luigi_utils.py```)
- when two tasks do not share dependencies they can run in parallel

A standard ```luigi.Task``` is run when its outputs do not yet exist. By subclassing it and overwrite its ```complete()``` method, one can control this behaviour. This is done in ```luigi_utils.py```.

To run the workflow, please type the following:

```shell
LUIGI_CONFIG_PATH=luigi.cfg; python run_workflow.py --user <lxplus username> --scheduler <local|central> --force <#> --workers <#> --tag <some_tag>
```

If one chooses ```--scheduler central```, one has to run ```luigid &``` first, and can control the number of workers to be used via ```--workers <number_of_workers>```. 

To force tasks to run, even if their output files already exist, use ```--force VALUE```, where ```VALUE``` ranges form 0 to the total number of tasks in the worflow. For instance, when ```VALUE=3```, the framework will force the three higher-level tasks to run: testing, training and energy normalization. The default, ```VALUE=0```, implies no forceful run. Finally, by choosing ```--user```, one can decide where the output data will be saved.

The parameter ```--tag``` must be used. It will create a folder where all the outputs will be stored.

The default configuration of the worflow is defined in the ```luigi_cfg.py``` class and can be overridden in the ```luigi.cfg``` TOML file, as explained in [```luigi```'s docs](https://luigi.readthedocs.io/en/stable/configuration.html).

The explicit definition of each task's outputs is an essential feature of ```luigi```. To make the process more transparent, this worflow stores file names under a configurable folder (```targets``` parameter in ```Config(luigi.Config())```) which includes all outputs of all tasks. Again, a task is run only if at least one of its outputs is older than its requirements (see ```luigi_utils.py```), or if ```--force``` is used.

> **_COMMON ERROR:_** When further extending the worflow by adding more tasks or changing some of their targets, it is quite common to observe an error indicating ```RuntimeError: Unfulfilled dependency at run time: <task>```. This shows that the task did not produce all the targets as expected by its ```output()``` method, causing its ```complete()``` method to always fail. The first it does, the worflow assumes the task to be lacking some dependencies, and so it reruns it. By construction, as soon as ```complete()``` fails again the error has to be thrown. Search for a mismatch on the names of the files produced and those expected by ```output()```, which could be subtle, such as an extra ```\n```. 

- **Debugging**: by passing ```--debug_workflow```, the user can obtain more information regarding the specific order tasks and their functions are run.

- **Visualizing the workflow**: when using ```--scheduler central```, one can visualize the ```luigi``` workflow by accessing the correct port in the browser, specified with ```luigid --port <port_number> &```. If using ```ssh```, the port will have to be forwarded to the local machine by using, for instance:

```shell
ssh -L <port_number>:localhost:<port_number> <server_address>
```

You should then be able to visualize the worflow in your browser by going to ```localhost:<port_number>```.

------------------------------------

## Future steps

### Using ```law```

In case [```law```](https://github.com/riga/law) is required (to manage ```htcondor``` jobs), one can install it as follows, using ```conda```:


- Install a [miniconda release](https://docs.conda.io/en/latest/miniconda.html) (python 3.7 linux used here)
- Convert ```law``` to a conda package using ```conda-build``` (this will not be required as soon as ```law``` is made available in some ```conda``` channel):

```
#from the conda "base" environment
conda install conda-build
conda skeleton pypi law
conda-build law
```

- Create conda environment and install the [```law```](https://github.com/riga/law) and [```ROOT```](https://root.cern/install/#conda) packages:

```
conda create --name <name> python=3.9
conda activate <name>
conda install --use-local law #install the conda package created in the previous step

#install ROOT from the ```conda-forge``` channel
conda config --set channel_priority strict 
conda install -c conda-forge root
```
