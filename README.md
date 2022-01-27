## HH->bbtautau Resonant Analysis: Trigger Scale Factors Framework

This framework calculates and displays the following:

- 1D and 2D Data and MC efficiencies
- 1D and 2D trigger scale factors
- variables distributions

The processing starts from skimmed ([KLUB](https://github.com/LLRCMS/KLUBAnalysis)) Ntuples. The framework is managed by ```luigi``` (see ```run_workflow.py```).


Tasks:

1. binning (manual or equal width with upper 5% quantile removal)
2. filling efficiencies histograms using HTCondor
3. add all individual histograms together
4. local efficiency and scale factors calculations and plotting

Requirements:

- ```python 3.9``` (likely works on other Python 3 versions)
- ```luigi``` (available in ```CMSSW``` after running ```cmsenv``` and using ```python3```).


#### Luigi Workflow

Run the submission workflow (check the meaning of the arguments by adding ```--help```.):

```shell
python3 run_workflow.py --outuser <lxplus username> --tag <some tag> --data MET2018 --mc_process TT --triggers METNoMu120 IsoTau50 --submit
```

To run the remaining part of the (local) workflow (tasks 3 and 4), run the same command without the ```--submit``` flag.


| Output files              | Destination folder                                  |
|---------------------------|-----------------------------------------------------|
| ```ROOT```                | ```/data_CMS/cms/<llr username>/TriggerScaleFactors/<some tag>/```    |
| Submission                | ```$HOME/jobs/<some tag>/<process>/submission/```           |
| Condor (output and error) | ```$HOME/jobs/<some tag>/<process>/outputs/```              |
| Pictures (requires ```/eos/```) | ```/eos/home-b/<lxplus username>/www/TriggerScaleFactors/``` |


You can also run each ```luigi``` task separately by running its corresponding ```python``` scripts (all support ```--help```). For instance (running within a LLR machine):

```bash
python3 scripts/getTriggerEffSig.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_backgrounds_test11Jan22/ --outdir /data_CMS/cms/alves/TriggerScaleFactors/UL_v1 --sample SKIMfix_TT_fullyHad --isData 0 --file output_2.root --subtag _default --channels all etau mutau tautau mumu --triggers METNoMu120 IsoTau50 --variables mht_et mhtnomu_et met_et dau2_eta dau2_pt HH_mass metnomu_et dau1_eta dau1_pt HT20 --tprefix hist_ --binedges_fname /data_CMS/cms/alves/TriggerScaleFactors/UL_v1/binedges.hdf
```
Variables can be configured in ```luigi_conf/__init__.py```.

#### Cleanup

In order to avoid cluttering the local area with output files, a ```bash``` script was written to effortlessly delete them:

```
bash triggerClean.sh --tag <any tag>
```

with options:

- ```-d```: debug mode, where all commands are printed to the screen and nothing is run
- ```-f```: full delete, including data produced by the HTCondor jobs (this flag is required to avoid data deletions by mistake)
- ```--tag```: tag used when producing the files (remove this options to print a message displaying all tags used in the past which were not yet removed)

-------------------------------------

#### Notes on ```luigi```

The advantages of using a workflow management system as ```luigi``` are the following:

- the whole chain can be run at once
- the workflow is clearer from a code point of view
- the configuration is accessible from within a single file (```luigi.cfg```)
- when two tasks do not share dependencies they can run in parallel

##### Forcing tasks to run

To force tasks to run, even if their output files already exist, use ```--force VALUE```, where ```VALUE``` ranges form 0 to the total number of tasks in the worflow. For instance, when ```VALUE=3```, the framework will force the three higher-level tasks to run: testing, training and energy normalization. The default, ```VALUE=0```, implies no forceful run. Finally, by choosing ```--user```, one can decide where the output data will be saved.

A standard ```luigi.Task``` is run when its outputs do not yet exist. By subclassing it and overwrite its ```complete()``` method, one can control this behaviour. This is done in ```luigi_utils.py```. 
A task in the chain is run only if at least one output file of a task on which it depends is more recent that at least one of the output files it already produced (via the ```ForceableEnsureRecentTarget``` custom class in ```luigi_utils.py```)


The explicit definition of each task's outputs is an essential feature of ```luigi```. To make the process more transparent, this worflow stores file names under a configurable folder (```targets``` parameter in ```Config(luigi.Config())```) which includes all outputs of all tasks. Again, a task is run only if at least one of its outputs is older than its requirements (see ```luigi_utils.py```), or if ```--force``` is used.

##### Debugging

By passing ```--debug_workflow```, the user can obtain more information regarding the specific order tasks and their functions are run.

##### Visualizing the workflow

When using ```--scheduler central```, one can visualize the ```luigi``` workflow by accessing the correct port in the browser, specified with ```luigid --port <port_number> &```. If using ```ssh```, the port will have to be forwarded to the local machine by using, for instance:

```shell
ssh -L <port_number>:localhost:<port_number> <server_address>
```

You should then be able to visualize the worflow in your browser by going to ```localhost:<port_number>```.
