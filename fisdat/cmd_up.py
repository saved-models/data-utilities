from datetime          import datetime
from google.cloud      import storage
from google.cloud      import client as gc
from hashlib           import sha384
import argparse
import codecs
import copy
import logging
from os.path import isfile
from pathlib import PurePath
import time
from typing import Optional
import uuid

import rdflib.plugins.parsers.notation3
import urllib.error
import yaml.scanner

from linkml.generators.rdfgen        import RDFGenerator
from linkml.utils.schemaloader       import SchemaLoader
from linkml_runtime.dumpers          import RDFLibDumper, YAMLDumper
from linkml_runtime.loaders          import RDFLibLoader, YAMLLoader
from linkml_runtime.utils.schemaview import SchemaView

from fisdat            import __version__, __commit__
from fisdat.utils      import extension_helper, prefix_helper, job_table
from fisdat.data_model import TableDesc, ManifestDesc

## data read/write buffer size, 1MB
BUFSIZ=1048576

def upload_files (args    : [str]
                , files   : [str]
                , owner   : str
                , ts      : str
                , dry_run : bool) -> str:
    logging.debug (f"Called `upload_files (args = {args}, files = {files}, owner = {owner}, ts = {ts})'")
    
    gen_path = lambda owner, ts, extra : owner + "/" + ts + "/" + extra
    client   = storage.Client()
    bucket   = client.bucket(args.bucket)
    jobuuid  = str(uuid.uuid1())
    path     = gen_path (owner, ts, args.directory) if args.directory is not None else gen_path (owner, ts, jobuuid)
    if (not dry_run):
        for fname in files:
            if fname is not None:
                fpath = path + "/" + fname
                print (f"Uploading gs://{args.bucket}/{fpath} ...")
                start = time.time ()
                blob = bucket.blob(fpath)
                blob.upload_from_filename (fname, timeout=86400)
                end = time.time ()
                abs_time = end - start
                if (abs_time < 1):
                    elapsed = round (abs_time, 2)
                else:
                    elapsed = round (abs_time)
                print (f"Uploaded {fname} in {elapsed}s")
    else:
        for fname in files:
            if fname is not None:
                fpath = path + "/" + fname
                print (f"Would upload to gs://{args.bucket}/{fpath} ...")
    return (jobuuid, f"gs://{args.bucket}/{path}")

def source () -> str:
    logging.debug ("Called `source()'")
    from google.cloud import client
    c   = client.Client ()
    res = c._credentials.service_account_email
    logging.info (f"GCP account e-mail: {res}")
    return res

def prep_index (manifest_path_yaml : str
              , manifest_path_ttl  : str
              , manifest_uri       : str
              , base_prefix        : str
              , index_name         : str
              , dry_run            : bool
    ) -> str:
    '''
    Echo the manifest file name to .index or other file.
    This avoids hard-coding the manifest title.
    '''
    logging.debug (f"Called `prep_index (manifest_path_yaml = {manifest_path_yaml}, manifest_path_ttl = {manifest_path_ttl}, manifest_uri = {manifest_uri}, index_name = {index_name})'")
    
    files = [ manifest_path_yaml, manifest_path_ttl, base_prefix, manifest_uri ]

    if (dry_run):
        inner_padding = max (map (lambda k : len(str(k)), files))
        padding_line = '-' * (inner_padding + 4)
        padded_lines = map (lambda k : "| " + str(k) + (' ' * (inner_padding - len(str(k)))) + " |", files)
        all_lines    = '\n'.join ([padding_line] + list(padded_lines) + [padding_line])
        print (f"Would have written the following to index file {index_name}:")
        print (all_lines)
    else:
        index_contents = f"{manifest_path_yaml}\n{manifest_path_ttl}\n{base_prefix}\n{manifest_uri}"
        output_index = codecs.open (index_name, "w", "utf-8")
        output_index.write (index_contents)
        output_index.close ()

    return (index_name)

# To-do: add fallback? Not added since it complicates things
def convert_feasibility (input_path  : str
                       , target_ext  : str
                       , target_path : Optional[str] = None
                       , force       : bool = True) -> (bool, PurePath):
    '''
    Helper function which returns whether a given filesystem operation is
    feasible.

    This doesn't do the write operation proper because it is useful both
    when renaming a file extension named wrongly (YAML data named `.ttl'
    or similar), and when dumping to a target file.
    
    It is unclear about whether some sort of fallback behaviour is desirable
    '''
    input_path_pure = PurePath (input_path)

    if target_path is None:
        logging.debug (f"Called `convert_feasibility (input_path = {input_path}, target_ext = {target_ext}, force = {force})'")
        output_path_pure = PurePath (input_path_pure.with_suffix (f".{target_ext}"))
    else:
        # Don't bother echoing `target_ext' as we don't use it if `target_path' exists
        logging.debug (f"Called `convert_feasibility (input_path = {input_path}, target_path = {target_path}, force = {force})'")
        output_path_pure = PurePath (target_path)
        
    #if (output_path_pure.suffix == input_path_pure.suffix):
    #    print (f"Target extension {target_ext} is same as input, won't do anything")
    #    stage_write = (False, output_path_pure)
    if (not isfile (output_path_pure)):
        print (f"Target file {output_path_pure} doesn't exist, so overwrite")
        stage_write = (True, output_path_pure)
    elif (isfile (output_path_pure) and force):
        print (f"Target file {output_path_pure} exists, and force-overwrite (`--force' option) is set, so overwrite")
        stage_write = (True, output_path_pure)
    else: #elif (isfile (target_file) and not force):
        print (f"Target file {output_path_pure} exists, but force-overwrite (`--force' option) is not set, so don't overwrite!")
        stage_write = (False, output_path_pure)

    return (stage_write)

def coalesce_schema (schema_path_yaml : str
                   , dry_run          : bool
                   , force            : bool
                   , schema_path_ttl  : Optional[str] = None
                   , conversion_stem  : str           = "converted"
    ) -> (bool, PurePath):
    '''
    Convert YAML schema to turtle equvialent
    '''
    logging.debug (f"Called `coalesce_schema (schema_path_yaml = {schema_path_yaml}, schema_path_ttl = {schema_path_ttl}'")

    (feasible, target_path_ttl) = convert_feasibility (input_path  = schema_path_yaml
                                                     , target_path = schema_path_ttl
                                                     , target_ext  = f"{conversion_stem}.ttl"
                                                     , force       = force)
    if (dry_run and feasible):
        print (f"Would have converted schema from YAML {schema_path_yaml} to TTL {target_path_ttl}")
        return (True, target_path_ttl)
    elif (feasible):
        print (f"Proceed with loading schema {schema_path_yaml}")
        try:
            target_schema_obj = SchemaLoader (schema_path_yaml)
            print ("Generating RDF from provided schema")
            generator = RDFGenerator (schema = target_schema_obj.schema)#, schemaview = schema_view)
            print ("Done generating RDF from provided schema, serialising")
            schema_ttl_description = generator.serialize()
        
            print (f"Dumping generated RDF to {target_path_ttl}")

            schema_output_ttl = codecs.open (target_path_ttl, "w", "utf-8")
            schema_output_ttl.write (schema_ttl_description)
            schema_output_ttl.close ()
            print (f"Successfully dumped generated RDF to {target_path_ttl}")
 
            return (True, target_path_ttl)
        except yaml.scanner.ScannerError:
            print (f"Conversion of YAML schema {schema_path_yaml} to TTL {target_path_ttl} is not feasible. Is it a valid YAML file?")
            return (False, target_path_ttl)
    else:
        print (f"Conversion of schema from YAML {schema_path_yaml} to TTL {target_path_ttl} is not feasible!")
        return (False, target_path_ttl)
    
def coalesce_table (tab      : TableDesc
                  , fake_cwd : str
                  , dry_run  : bool
                  , force    : bool
                  , convert  : bool
                  , stem     : str) -> (bool, TableDesc):
    '''
    '''
    logging.debug (f"Called `coalesce_table (tab = {tab}, fake_cwd = {fake_cwd}, force = {force}, convert = {convert}, stem = {stem})'")
    
    table_uri      = tab.resource_path
    fake_table_uri = f"{fake_cwd}{table_uri}"
    extant_uri     = isfile (fake_table_uri)

    if (not isfile (fake_table_uri)):
        print (f"Error: target file {fake_table_uri} does not exist!")
        return (False, tab)
    else:
        with open (fake_table_uri, "rb") as fp:
            data = fp.read ()
            hash = sha384  (data)
                
            if hash.hexdigest() != tab.resource_hash:
                print (f"{fake_table_uri} has changed, please revalidate with `fisdat'")
                return (False, tab)
        if convert:
            '''
            Setting force=True always is a hack for now because without
            setting this, if the schema is shared by multiple data
            tables, then it fails for the second one. 
            A robust fix for this will go back to the data model, because
            the schema itself is a resource and this is the only way to
            avoid conversion more than once.
            In any case, it is useful to echo the target TTL path through
            a function not unlike this one. Furthermore, there may be
            more than one notion of feasibility, beyond 'does this path
            exist' as we have now.
            '''
            fake_path_yaml             = f"{fake_cwd}{tab.schema_path_yaml}"
            (schema_success, path_ttl) = coalesce_schema (schema_path_yaml = fake_path_yaml
                                                        , dry_run          = dry_run
                                                        , force            = True
                                                        , conversion_stem  = stem)
            if (schema_success):
                tab.schema_path_ttl = path_ttl.name
            return (schema_success, tab)
        else:
            # Should return True here as manifest conversion is feasible
            # but we've just not subbed in the TTL conversion filename
            return (True, tab)

    
def coalesce_manifest (manifest_path   : str
                     , manifest_format : str
                     , data_model_uri  : str
                     , prefixes        : dict[str, str]
                     , gcp_source      : str
                     , dry_run         : bool
                     , force           : bool
                     , convert_schema  : bool = True
                     , conversion_stem : str  = "converted"
                     , fake_cwd        : str  = ""
    ) -> (bool, Optional[ManifestDesc], Optional[PurePath], Optional[PurePath], Optional[str]):
    '''
    The YAML files are provided and edited locally, but we can't process
    these with non-Python tooling. This function converts schemata
    described in the manifest to turtle, then converts the manifest
    itself to turtle.

    This actually became more complicated than I had originally
    imagined. A potted summary is as follows:

    1. Initial checks: target to load exists, data model URI is loadable
    2. Load the manifest with either the TTL or YAML loader.
    3. Extract/expand manifest URI (from e.g. `rap:RootManifest')
    4. Validate/convert tables in the manifest file, providing that
       neither `dry_run' is set nor `validate' is unset.
    5. Convert the manifest file to TTL
    '''
    logging.debug (f"Called `coalesce_manifest (manifest_path = {manifest_path}, data_model_uri = {data_model_uri}, prefixes = {prefixes}, gcp_source = {gcp_source})'")

    dumper_ttl = RDFLibDumper ()
    dumper_yml = YAMLDumper ()
    loader_ttl = RDFLibLoader ()
    loader_yml = YAMLLoader ()
    '''
    1. Initial validation of arguments
    '''
    if not isfile (manifest_path):
        print (f"Manifest file {manifest_path} does not exist!")
        return (False, None, None, None, None)

    try:
        py_data_model_view = SchemaView (data_model_uri)
    except urllib.error.HTTPError as e:
        print (f"HTTP error {e.code} trying data model URI `{e.url}'")
        print ("If you've overridden the default using the `--data-model-uri' option, double-check that it's valid.")
        return (False, None, None, None, None)

    '''
    2. Load manifest with either TTL or YAML loader
       Turtle is what is actually read, so also check feasibility of creating
       a *new* TTL manifest which will include the `schema_ttl' attribute
    '''
    if (manifest_format == "ttl"):
        try:
            manifest_obj = loader_ttl.load (source       = manifest_path
                                          , target_class = ManifestDesc
                                          , schemaview   = py_data_model_view)
            (annotated_feasible, annotated_path_ttl) = convert_feasibility (
                input_path = PurePath (manifest_path)
              , target_ext = "annotated.ttl"
              , force      = force
            )
            
            (conv_feasible, manifest_path_yaml) = convert_feasibility (
                input_path = PurePath (manifest_path)
              , target_ext = f"{conversion_stem}.yaml"
              , force      = force
            )
            # Always return the annotated path, and above two must succeed
            manifest_path_ttl = annotated_path_ttl
            manifest_feasible = conv_feasible and annotated_feasible
        except rdflib.plugins.parsers.notation3.BadSyntax:
            print (f"Cannot load file {manifest_path} with the RDF/TTL loader. Is your manifest a YAML manifest? (\"yaml\" `--manifest-format' option)")
            return (False, None, None, None, None)

    elif (manifest_format == "yaml"):
        try:
            manifest_obj = loader_yml.load (source       = manifest_path
                                          , target_class = ManifestDesc)

            manifest_path_yaml = PurePath (manifest_path)
            (manifest_feasible, manifest_path_ttl) = convert_feasibility (
                input_path      = manifest_path_yaml
              , target_ext      = f"{conversion_stem}.ttl"
              , force           = force
            )
        except yaml.scanner.ScannerError:
            print (f"Cannot load file {manifest_path} with the YAML loader. Is your manifest an RDF/TTL manifest? (\"ttl\" `--manifest-format' option)")
            return (False, None, None, None, None)
    else:
        print (f"Unrecognised serialisation mode {manifest_format}, cannot load extant object")
        return (False, None, None, None, None)

    '''
    3. Extract/expand manifest URI
    '''
    manifest_uri = prefix_helper (py_data_model_view.schema,
                                  manifest_obj.atomic_name,
                                  prefixes["_base"])
    logging.debug(f"Manifest URI is {manifest_uri}")

    '''
    4. Validate/convert tables in manifest file

    This bit is really annoying in the sense that we want to only update
    the tables if all the schema converted successfully. Running the map
    over the list is effectual so need to use the .copy() method to
    create a new, isolated list.
    '''
    if (manifest_feasible):
        logging.debug (f"Original manifest tables: {manifest_obj.tables}")
        copied_obj   = copy.deepcopy (manifest_obj)
        rough_tables = map (lambda t : coalesce_table(t, fake_cwd, dry_run, force, convert_schema, conversion_stem), copied_obj.tables)
        tables_signals, tables_results = zip(*rough_tables)
        logging.debug (f"Table signals: {tables_signals}")
        logging.debug (f"Table results: {tables_results}")

        if (all (tables_signals)):
            print ("Successfully converted all schemata from YAML to TTL")
            manifest_obj.tables = list (tables_results)
        else:
            print ("Conversion of some schemata from YAML to TTL failed, or hashes were invalid, see previous messages")
            manifest_feasible = False
    '''
    Convert manifest from YAML to TTL, or vice versa
    '''
    if (dry_run and manifest_feasible):
        if (manifest_format == "ttl"):
            print (f"Would have converted manifest from TTL {manifest_path_ttl} to YAML {manifest_path_yaml}")
        else:
            print (f"Would have converted manifest from YAML {manifest_path_yaml} to TTL {manifest_path_ttl}")

    elif (dry_run):
        if (manifest_format == "ttl"):
            print (f"Conversion of manifest from TTL {manifest_path_ttl} to YAML {manifest_path_yaml} would not have been feasible!")
        else:
            print (f"Conversion of manifest from YAML {manifest_path_yaml} to TTL {manifest_path_ttl} would not have been feasible!")
    
    elif (manifest_feasible):
        dumper_ttl = RDFLibDumper()
        dumper_yml = YAMLDumper()
        if (manifest_format == "ttl"):
            dumper_yml.dump (manifest_obj, manifest_path_yaml)
            dumper_ttl.dump (manifest_obj, manifest_path_ttl
                           , schemaview = py_data_model_view
                           , prefix_map = prefixes)
            return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
        else:
            dumper_ttl.dump (manifest_obj, manifest_path_ttl
                           , schemaview = py_data_model_view
                           , prefix_map = prefixes)   
        print (job_table (manifest_obj, preamble = False, mode = 'r'))
    
    else:
        if (manifest_format == "ttl"):
            print (f"Conversion of manifest from TTL {manifest_path_ttl} to YAML {manifest_path_yaml} was not feasible!")
        else:
            print (f"Conversion of manifest from YAML {manifest_path_yaml} to TTL {manifest_path_ttl} was not feasible!")

    return (manifest_feasible, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
        
    
def cli () -> None:
    """
    Command line interface
    """
    tmploc = "https://rap.tardis.ac/saved"
    
    print (f"This is fisup version {__version__}, commit {__commit__}")
    
    parser = argparse.ArgumentParser("fisup")
    verbgr = parser.add_mutually_exclusive_group (required = False)

    parser.add_argument ("-u", "--unsecure"
                       , action="store_true"
                       , help="Disable SSL validation")
    parser.add_argument ("-b", "--bucket"
                       , default="saved-fisdat"
                       , help="Bucket to upload into")
    parser.add_argument ("-d", "--directory"
                       , help="Directory within bucket to upload into"
                       , default = None)
    parser.add_argument ("-s", "--source"
                       , help="Data source email"
                       , default = None)
    parser.add_argument ("manifest", help="Manifest file")
    parser.add_argument ("--index"
                       , help = "Name of hidden index file recording manifest file name"
                       , default = ".index")
    parser.add_argument ("--data-model-uri"
                       , help     = "Data model YAML specification URI"
                       , default  = "https://marine.gov.scot/metadata/saved/schema/meta.yaml")
    parser.add_argument ("-f", "--manifest-format", "--serialisation"
                       , help     = "Input manifest format"
                       , type     = str
                       , choices  = ["yaml", "ttl"]
                       , default  = "yaml")
    parser.add_argument ("--base-prefix"
                       , help     = "RDF `@base' prefix from which manifest, results, data and descriptive statistics may be served."
                       , default  = "https://marine.gov.scot/metadata/saved/rap/")
    parser.add_argument ("--no-convert-schema", "--no-validate-schema", "--no-validate"
                       , help     = "Disable schema validation/conversion when converting manifest"
                       , action   = "store_true"
                       , default  = False)
    parser.add_argument ("-n", "--no-upload"
                       , help     = "Don't upload files"
                       , action   = "store_true"
                       , default  = False)
    parser.add_argument ("--dry-run"
                       , help     = "Simulate program operation"
                       , action   = "store_true"
                       , default  = False)
    parser.add_argument ("-F", "--force"
                       , help = "Forcibly overwrite files in case of conflicts"
                       , action = "store_true"
                       , default = False)
    verbgr.add_argument ("-v", "--verbose"
                       , help     = "Show more information about current running state"
                       , required = False
                       , action   = "store_const"
                       , dest     = "log_level"
                       , const    = logging.INFO)
    verbgr.add_argument ("-vv", "--extra-verbose"
                       , help     = "Show even more information about current running state"
                       , required = False
                       , action   = "store_const"
                       , dest     = "log_level"
                       , const    = logging.DEBUG)
    args = parser.parse_args ()
    
    logging.basicConfig (level  = args.log_level
                       , format = "%(levelname)s [%(asctime)s] [`%(filename)s\' `%(funcName)s\' (l.%(lineno)d)] ``%(message)s\'\'")
        
    if args.unsecure:
        from rdflib import _networking
        from fisdat import kludge

        _networking._urlopen = kludge._urlopen

    # Sub this into `coalesce_manifest()'
    if (args.source is None):
        data_source_email = source ()
    else:
        data_source_email = args.source

    prefixes = { "_base": args.base_prefix
               , "rap"  : "https://marine.gov.scot/metadata/saved/rap/"
               , "saved": "https://marine.gov.scot/metadata/saved/schema/" }

    # Case 1: no_upload is set -> set both
    # Case 2: no_upload is not set, set 
    #if args.no_upload:
    #    print ("No upload (`--no-upload') option is set, neither validate/convert schema nor upload files")
    #    no_validate = True
    #    dry_run     = True
    #else:
    #    no_validate = args.no_validate
    #    dry_run     = args.no_upload
    convert_schema = not args.no_convert_schema
    dry_run        = args.dry_run
    no_upload      = args.dry_run or args.no_upload

    (test_signal, manifest_obj, manifest_yaml, manifest_ttl, manifest_uri) = coalesce_manifest (
            manifest_path   = args.manifest
          , manifest_format = args.manifest_format
          , data_model_uri  = args.data_model_uri
          , prefixes        = prefixes
          , gcp_source      = data_source_email
          , dry_run         = dry_run
          , convert_schema  = convert_schema
          , force           = args.force
        )
    
    if (test_signal):
        index = prep_index (manifest_path_yaml = manifest_yaml
                          , manifest_path_ttl  = manifest_ttl
                          , manifest_uri       = manifest_uri
                          , base_prefix        = args.base_prefix
                          , index_name         = args.index
                          , dry_run            = dry_run)
        
        resources     = [table.resource_path    for table in manifest_obj.tables]
        schemata_ttl  = [table.schema_path_ttl  for table in manifest_obj.tables]
        schemata_yaml = [table.schema_path_yaml for table in manifest_obj.tables]
        time_stamp    = datetime.today ().strftime ('%Y%m%d')
        short_name    = data_source_email.split ('@') [0]

        staging_files = [str (manifest_yaml)
                       , str (manifest_ttl)
                       , index] + resources + schemata_yaml + schemata_ttl
        
        uuid, url = upload_files (args, staging_files, short_name, time_stamp, no_upload)

        if (no_upload):
            print(f"Would have uploaded your data/job set/bundle to {url}")
        else:
            print(f"Successfully uploaded your data/job set/bundle to {url}")
            print(f"Result should, within the next 5-10 minutes, appear at {tmploc}/rap/{uuid}/")

