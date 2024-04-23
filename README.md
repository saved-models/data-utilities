# Fish Data Utilities

## Pre-requisites:

After we integrated LinkML, there is an external dependency on our
work-in-progress data model. This is stored in an external repository,
so make sure to run the following:

    git submodule init && git submodule update

This is a Python package. It can be installed in any of the usual ways
for Python packages, perhaps using a virtual environment like so:

    python -m venv /some/where/env
    source /some/where/env/bin/activate

Whence installing the utilities is done as:

	pip install --editable .

The `--editable` or `-e` flag is important as it means that updates
to the file (i.e. those fetched with git) run immediately. Having done
this, some new programs are available:

## fisdat - validating and working with data files
### Operation

The `fisdat` program is for preparing data files to be published. 
It takes a CSV file and a schema and checks that the CSV file matches
the schema. It then adds the file and schema to a manifest.

For example, in the `examples/sentinal_cages` directory, one can
run,

	fisdat sentinel_cages_sampling.yaml \
	    sentinel_cages_cleaned.csv \
		manifest.ttl

which will result in a slew of warnings about entries in the file
that do not match the datatype specified in the schema (adding the
`-s` or `--strict` flag will turn these warnings into errors) and
result in a new or updated `manifest.json` file which serves to
indicate which data belongs to which schema.

If you do /not/ wish to validate the data file, perhaps because
it is not a CSV file, you can give the program the `-n` argument.

Do this for each file that should be added to the manifest.

### Dealing with missing data (important for validation)

In the sentinel cages example data, empty/missing values are indicated
using the string "NA". LinkML is unable to accept these as empty (we
have opened an issue to try and move this forward). In the meantime,
LinkML will happily accept empty fields. In the sentinel cages data,
we have added an example R script called `prep.R` which will read in
the CSV, then re-export a new table with the NA string as "".

### Debugging / extra information about running state

Providing the `--verbose` flag (or `-v` for short) will print messages
about running state, e.g.:

	fisdat sentinel_cages_sampling.yaml \
	    sentinel_cages_cleaned.csv \
		manifest.ttl \
		--verbose

To see even more information, use the `--extra-verbose` (or `-vv` for 
short), e.g.: 

	fisdat sentinel_cages_sampling.yaml \
	    sentinel_cages_cleaned.csv \
		manifest.ttl \
		--extra-verbose

Program version number and associated git commit is always printed.

## fisup - uploading data
### Operation

Once the manifest is full, uploading the data can be done with the
program `fisup`. It is used like this,

	fisup manifest.ttl

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

	$ fisdat fo_farms.yaml fo_farms.csv manifest.ttl
	$ fisdat fo_lice.yaml fo_lice_data.csv manifest.ttl           
	$ fisup manifest.ttl
	Uploading gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562/manifest.ttl ...
	Uploading gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562/fo_farms.csv ...
	Uploading gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562/fo_lice_data.csv ...
	Successfully uploaded your dataset to gs://saved-fisdat/2d6bf8f4-c6cc-11ee-9969-7aa465704562

Now the dataset bundle has been uploaded and can be further
processed.

### Usage notes

Neither the name nor file extension of the manifest matter. They are
always serialised as RDF (TTL). However, older manifests in JSON can
no longer be uploaded, so make sure to re-generate them.

The `--verbose` and `--extra-verbose` flags have the same effect as in
`fisdat`. They print debugging information about running state. 
Similarly, the version number and associated git commit are always
printed.

## LinkML YAML usage

Many of the LinkML schema fields are vague. 

### The `id` and `name` fields
The `id` field must be an URI, pointing somewhere. This does not need to be active, e.g. I put 'http://localhost/marinescot/sentinel_cages/sampling' in one of the examples.

The `name` field is a short identifier or 'atom'. It cannot have spaces or most special characters, albeit underscores are valid. Put longer text titles in the `title` field, and longer still free text descriptions in the `description` field. (Unlike `id` and `name`, the `description` field is optional.)

### Prefixes and imports

Prefixes in the LinkML schema are used as the start of URIs in the generated schema. In the sentinel cages YAML example, we define `saved` as one such prefix, and then set it as the default prefix with the `default_prefix` keyword. The effect of this is that, by default, the classes and slots have a URI prepended to them in the generated documentation, which is this default prefix.

For example, suppose we declared a slot called `infection_pressure`, declare a prefix `saved_new` with URI "http://localhost/saved_new/", and set `saved_new` as the value of `default_prefix`. The slot `infection_pressure` would then be given the URI `saved_new:infection_pressure` which would expand to "http://localhost/saved_new/infection_pressure".

The imports take a prefix and import resources from it. It is sufficient to leave this as in the examples for now, as `linkml:types` and our own schema declare everything we need.

### URI and CURIE prefixes

These are in the format `prefix:atom`. There must be no space on either side of the colon. These are typically used in the various mappings attributes of slots, or overriding an URI.

### Indentation

Indentation does matter in most circumstances, because it is how the YAML distinguishes between sections. Getting the indentation right also makes the document easier to read, albeit it can sometimes be difficult to see where the indentation is wrong.

Indentation may take any number of spaces, the suggested number is two or four.
