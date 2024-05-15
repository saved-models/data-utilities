from linkml.generators.pythongen     import PythonGenerator
from linkml.utils.schemaloader       import SchemaLoader
from linkml.utils.schema_builder     import SchemaBuilder
from linkml_runtime.dumpers          import RDFLibDumper, YAMLDumper
from linkml_runtime.linkml_model     import SchemaDefinition
from linkml_runtime.loaders          import RDFLibLoader, YAMLLoader
from linkml_runtime.utils.schemaview import SchemaView

import argparse
from hashlib    import sha384
from os.path    import isfile
from pathlib    import Path, PurePath
import inspect
import logging

from fisdat            import __version__, __commit__
from fisdat.data_model import ColumnDesc, JobDesc, TableDesc, ManifestDesc
from fisdat.ns         import CSVW
from fisdat.utils      import fst, error, extension_helper, job_table, schema_components_helper, expand_schema_components, take, validation_helper

def dump_wrapper (py_obj
                , data_model_view : SchemaView
                , output_path     : PurePath
                , prefixes        : dict[str, str]
                , mode            : str) -> bool:
    '''
    Given a Python object to serialise, and a SchemaView object derived
    from the data model, serialise RDF.
    While `get_dumper' is generic and will happily select the right
    dumper function based on the output path's file extension, the
    RDF and JSON-LD serialisers unfortunately don't accept the same
    schemaview context, instead for JSON-LD, one has to invoke
    `get_contexts' and provide the resulting context to the conexts
    argument.
    There was strange behaviour when calling RDFDumper.dumper directly,
    which is why it's not called directly.
    '''
    logging.debug (f"Called `dump_wrapper (py_obj = {py_obj}, data_model_view = {SchemaView}, output_path = {str(output_path)}, prefixes = {prefixes}, mode = {mode})'")
    
    output_path_abs = str (output_path.name)
    output_path_ext = extension_helper (output_path)

    namespaces = data_model_view.namespaces()
    logging.info (f"Namespaces in the data model are {namespaces}")

    if (mode == "rdf_ttl_manifest"):
        if (output_path_ext != "rdf" and output_path_ext != "ttl"):
            logging.info (f"Warning: target extension has a .{output_path_ext} extension, but will actually be serialised as RDF/TTL")
        dumper = RDFLibDumper ()
        
        logging.info (f"Dumping Python object to {output_path_abs}")
        # Hard-coded at the moment, need to fix this
        dumper.dump (py_obj, output_path_abs, schemaview = data_model_view, prefix_map = prefixes)

        return (True)            
        
    elif (mode == "yaml_template"):
        if (output_path_ext != "yaml" and output_path_ext != "yml"):
            logging.info (f"Warning: target extension has a .{output_path_ext} extension, but will actually be serialised as YAML")
        dumper = YAMLDumper ()
        
        logging.info (f"Dumping Python object to {output_path_abs}")
        dumper.dump (py_obj, output_path_abs)

        return (True)
    
    else:
        print ("Unrecognised mode for `dump_wrapper()', cannot dump object")
        return (False)

def append_job_manifest (data           : str
                       , schema         : str
                       , data_model_uri : str
                       , manifest       : str
                       , manifest_title : str
                       , mode           : str
                       , scoped_columns : [str]
                       , prefixes       : dict[str, str]) -> bool:
    '''
    Given a data file, a file schema, and the parent data model, build
    up a Python object which can be serialised to RDF.
    The data model is necessary as it provides JSON-LD contexts, which
    are ncessary when serialising JSON-LD and RDF. It can point to
    job.yaml or the meta-model which pulls it in at the top-level.
    '''
    logging.debug (f"Called `append_job_manifest (data = {data}, schema = {schema}, data_model_uri = {data_model_uri}, manifest = {manifest}, manifest_title = {manifest_title}, mode = {mode}, scoped_columns = {scoped_columns}, prefixes = {prefixes})'")
    
    manifest_path   = PurePath (manifest)
    manifest_ext    = extension_helper (manifest_path)
    data_path       = PurePath (data) # Necessary to only include the file name proper
    schema_path     = PurePath (schema) # ''

    # Note, even before calling this function, the file is known to exist
    with open (data, "rb") as fp:
        data_text = fp.read ()
    data_hash = sha384 (data_text).hexdigest()
    
    py_data_model_view = SchemaView (data_model_uri)
    
    logging.info ("Generating base job description")
    schema_obj        = SchemaLoader (schema).schema
    schema_properties = schema_components_helper (schema_obj)
    target_set_atomic = schema_properties ["atomic_name"]
    
    logging.info ("Polling for scope components")
    scope_triple = expand_schema_components (schema_obj
                                           , schema_properties
                                           , scoped_columns
                                           , prefixes)
    
    logging.info ("Generating base table description")
    staging_table = TableDesc (
        atomic_name   = target_set_atomic
      , title         = schema_properties ["title"]
      , description   = schema_properties ["description"] # Partly for filling out a template, use even empty
      , resource_path = data_path.name
      , schema_path   = schema_path.name
      , resource_hash = data_hash
    )
    logging.debug (f"Base table description is `{staging_table}'.")

    logging.info ("Generating base example job description")
    
    initial_example_job = JobDesc (
        atomic_name           = f"job_example_{target_set_atomic}"
      , title                 = f"Empty job template for {target_set_atomic}"
      , job_type              = prefixes["_base"] + "job_type_ignore"
      , job_scope_descriptive = scope_triple["descriptive"]
      , job_scope_collected   = scope_triple["collected"]
      , job_scope_modelled    = scope_triple["modelled"]
    )
    logging.debug (f"Base example job description is `{initial_example_job}'. Its nominal type is {type(initial_example_job)}")
    
    logging.info ("Proceeding with manifest initialise or append operation")
    if (mode == "initialise"):        
        logging.info (f"Initialising manifest {manifest}")
        manifest_skeleton = ManifestDesc (
              atomic_name   = manifest_title
            , tables        = [staging_table]
            , jobs          = [initial_example_job]
            , local_version = __version__
        )
        result = dump_wrapper (py_obj          = manifest_skeleton
                             , data_model_view = py_data_model_view
                             , output_path     = manifest_path
                             , prefixes        = prefixes
                             , mode            = "rdf_ttl_manifest")
        
        print (job_table (manifest_skeleton, manifest, preamble = True))

    else:
        logging.info (f"Reading existing manifest {manifest}")
        loader          = RDFLibLoader ()
        extant_manifest = loader.load (source       = manifest
                                     , target_class = ManifestDesc
                                     , schemaview   = py_data_model_view)
        logging.info (f"Checking that data file {data} does not already exist in manifest")
        extant_paths      = map (lambda k : PurePath (k.resource_path).name, extant_manifest.tables)
        check_extant_path = data_path.name in extant_paths
        if (check_extant_path):
            print (f"Data-file {data} was already in the table, cannot add!")
            result = (not check_extant_path) and (not check_extant_name)
        else:
            logging.info (f"Data-file {data} was not in manifest, adding")
            '''
            Don't bother adding an additional empty job here, since the 
            actual aim of creating the empty or example job in the
            initialisation stage is to make sure that the job field is
            minimally filled out.
            
            These fields are marked as required in `job.yaml', and we've
            just read in the manifest using that component of the data
            model as the manifest's *schema*. Therefore, it's safe to
            assume that they are filled out if we get this far.
            Further update the local utility version string to that of
            the most recent time we run it.
            
            Copy the loaded `extant_manifest' into a new object to work
            on. This is not particularly important at the moment but
            does aid debugging and makes clear that we're not writing
            the original object back, especially if we make more changes
            than appending to the table and updating the local version
            string.
            '''
            staging_manifest = extant_manifest
            staging_manifest.tables.append (staging_table)
            staging_manifest.local_version = __version__
            
            result = dump_wrapper (py_obj          = staging_manifest
                                 , data_model_view = py_data_model_view
                                 , output_path     = manifest_path
                                 , prefixes        = prefixes
                                 , mode            = "rdf_ttl_manifest")

            print (job_table (staging_manifest, manifest, preamble = True))
            
    return (result)

def manifest_wrapper (data           : str
                    , schema         : str
                    , data_model_uri : str
                    , manifest       : str
                    , manifest_title : str
                    , validate       : bool
                    , scoped_columns : list[str]
                    , prefixes       : dict[str, str]) -> bool:
    '''
    Simple wrapper for the two modes of `append_job_manifest' based on
    whether the manifest file exists (optional) and whether the schema
    and data file exists (obviously mandatory).
    '''
    logging.debug (f"Called `manifest_wrapper (data = {data}, schema = {schema}, data_model_uri = {data_model_uri}, manifest = {manifest}, manifest_title = {manifest_title}, validate = {validate}, scoped_columns = {scoped_columns}, prefixes = {prefixes})'")
    logging.debug (f"Checking that input data {data} and schema {schema} files exist")
    
    prereq_check = isfile (data) and isfile (schema)
    
    if (isfile (data) and isfile (schema)):
        if (validate):
            validation_check = validation_helper (data, schema, "TableSchema")
        else:
            logging.info (f"Validation of data-file {data} against schema {schema} disabled")
            validation_check = True
            
        if (validation_check):
            if (isfile (manifest)):
                logging.info (f"Manifest exists, appending to manifest {manifest}")
                result = append_job_manifest (data, schema, data_model_uri
                                            , manifest, manifest_title, "append"
                                            , scoped_columns, prefixes)
            else:
                logging.info (f"Manifest does not exist, creating new manifest {manifest}")
                result = append_job_manifest (data, schema, data_model_uri
                                            , manifest, manifest_title, "initialise"
                                            , scoped_columns, prefixes)
            return (result)
        else:
            '''
            `validation_helper' already returns informative error
            messages, so just return its boolean signal
            '''
            return (validation_check)
    else:
        if (not isfile (data) and not isfile (schema)):
            print (f"Neither data file {data} nor schema file {schema} exist!")
        elif (not isfile (data)):
            print (f"Data file {data} does not exist!")
        elif (not isfile (schema)):
            print (f"Schema file {schema} does not exist!")
        return (prereq_check)


    
def cli () -> None:
    print (f"This is fisdat version {__version__}, commit {__commit__}")
    
    parser = argparse.ArgumentParser ("fisdat")
    verbgr = parser.add_mutually_exclusive_group (required = False)
    parser.add_argument ("schema"  , help = "Schema file/URI (YAML)", type = str)
    parser.add_argument ("csvfile" , help = "CSV data file", type = str)
    parser.add_argument ("manifest", help = "Manifest file", type = str)
    parser.add_argument ("-n", "--no-validate", "--dry-run"
                       , help     = "Disable validation"
                       , action   = "store_true")
    parser.add_argument ("--data-model-uri"
                       , help     = "Data model YAML specification URI"
                       , default  = "https://marine.gov.scot/metadata/saved/schema/meta.yaml")
    parser.add_argument ("--job-scope"
                       , help     = "Use these columns to fill out example job section in initial manifest"
                       , metavar  = 'COL'
                       , nargs    = '+'
                       , default  = [])
    parser.add_argument ("--manifest-title"
                       , help     = "Name of the manifest title root"
                       , default  = "RootManifest")
    parser.add_argument ("--base-prefix"
                       , help     = "@base prefix from which job manifest, job results, data and descriptive statistics may be served."
                       , default  = "https://marine.gov.scot/metadata/saved/rap/")
    parser.add_argument ("--saved-prefix"
                       , help     = "`saved' data model schema prefix"
                       , default  = "https://marine.gov.scot/metadata/saved/schema/")
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

    logging.debug (f"Columns selected to bring into job scope are f{args.job_scope}")

    prefixes = { "_base": args.base_prefix
               , "saved": args.saved_prefix}

    manifest_wrapper (data           = args.csvfile
                    , schema         = args.schema
                    , data_model_uri = args.data_model_uri
                    , manifest       = args.manifest
                    , manifest_title = args.manifest_title
                    , validate       = not args.no_validate
                    , scoped_columns = args.job_scope
                    , prefixes       = prefixes)

