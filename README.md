# Fish Data Utilities

## Pre-requisites:

The data model is stored in an external repository. Make sure to run

    git submodule init && git submodule update

This is a Python package. It can be installed in any of the usual ways
for Python packages, perhaps using a virtual environment like so,

    python -m venv /some/where/env
	. /some/where/env/bin/activate

whence installing the utilities is done as,

    python setup.py install

having done this, some new programs are available:

## fisdat - validating and working with data files

The `fisdat` program is for preparing data files to be published. 
It takes a CSV file and a schema and checks that the CSV file matches
the schema. It then adds the file and schema to a manifest.

For example, in the `examples/sentinal_cages` directory, one can
run,

	fisdat sampling_info.json \
	    Sentinel_cage_sampling_info_update_01122022.csv \
		manifest.json

which will result in a slew of warnings about entries in the file
that do not match the datatype specified in the schema (adding the
`-s` or `--strict` flag will turn these warnings into errors) and
result in a new or updated `manifest.json` file which serves to
indicate which data belongs to which schema.

If you do /not/ wish to validate the data file, perhaps because
it is not a CSV file, you can give the program the `-n` argument.

Do this for each file that should be added to the manifest.

## fisup - uploading data

Once the manifest is full, uploading the data can be done with the
program `fisup`. It is used like this,

	fisup manifest.json
	
You will need to set an environment variable to where you have
saved your access credentials. It needs to be the full path to
the file. If you do not have access credentials, you will need
to ask for them.

	export GOOGLE_APPLICATION_CREDENTIALS=/some/where/fisdat.key

It will do some basic checks on the files and then upload them to
cloud storage. Use the `-d` command line option to specify a 
particular directory path if you do not want one to be randomly
generated. It is a good idea to make a note of the generated 
path. For example, from the `examples/farm_site_af_source` 
directory,

	$ fisdat fo_farms.json fo_farms.csv manifest.json                   
	$ fisdat fo_lice_data.json fo_lice_data.csv manifest.json           
	$ fisup manifest.json 
	Checking fo_farms.csv ...
	Checking fo_lice_data.csv ...
	Uploading gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562/manifest.json ...
	Uploading gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562/fo_farms.csv ...
	Uploading gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562/fo_lice_data.csv ...
	Successfully uploaded your dataset to gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562

Now the dataset bundle has been uploaded and can be further
processed.
