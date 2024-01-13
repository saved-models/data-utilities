# Fish Data Utilities

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

## fisup - uploading data

This does not exist yet but will work from a manifest and will 
upload the manifest, the data files it refers to, and any schemata
to central storage.
