# harp-data-pipeline for CARRA2

Scripts to setup ecflow and archival of climate means for CARRA.
It also includes some scripts to setup sqlite files for harp verification


## bash

Wrappers to call some daily running scripts

### bash/archiving/ecf_submitters/
Contains scripts to produce means using ecflow.


Needs these files to be created before running them

```
cd bash/archiving/ecf_submitters/
ln -sf ../../config/config_archive.sh env.sh
```

### bash/archiving/archive_submitters

Contains scripts for archival.

Needs these files to be created before running them
on each directory

Example:
```
cd daily_mean_an
ln -sf ../../config/config_archive.sh env.sh
ln -sf ../../config/load_eccodes.sh load_eccodes.sh
```

## python

## go
