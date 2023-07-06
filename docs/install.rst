# ACCESS MOPPeR - A Model Output Post-Processor for the ACCESS climate model


MOPPeR processes 
MOPPeR is distributes via the ACCESS-NRI conda channel and github, however, MOPPeR was developed by the CLEX CMS team starting from the APP4 ().

Commands:

- **wrapper**  
- **table** 
- **cmor** 
- **check** 
- **map** 
- **template** 

-------
Install
-------

    You can install the latest version of `mopper` directly from conda (accessnri channel)

    conda install -c accessnri mopper 

    If you want to install an unstable version or a different branch:

    * git clone 
    * git checkout <branch-name>   (if installing a a different branch from master)
    * cd mopper 
    * python setup.py install or pip install ./ 
      use --user with either othe commands if you want to install it in ~/.local

---------------------
Working on NCI server
---------------------

MOPPeR is pre-installed into a Conda environment at NCI. Load it with::

    module use /g/data3/hh5/public/modules
    module load conda/analysis3-unstable

NB You need to be a member of hh5 to load the modules
