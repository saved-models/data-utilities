import argparse
from itertools  import chain
from os.path    import isfile
import logging

from linkml_runtime.loaders          import RDFLibLoader, YAMLLoader
from linkml_runtime.dumpers          import YAMLDumper  , RDFLibDumper
from linkml_runtime.utils.schemaview import SchemaView

#from fisdat            import __version__, __commit__
from fisdat.utils      import validation_helper
from fisdat.data_model import ManifestDesc

import pkg_resources
__version__ = pkg_resources.require("fisdat")[0].version

'''
Column descriptions have three elements:

1. The column proper. This can never come from a table *different* to the
   table in the same attribute, so any full URI for a given column is
   always redundant, at least in terms of the possibility that it may
   differ.
2. The underlying variable. The underlying variable, which is a
   well-known field in the data model, is used for pattern-matching, so
   it is always well-known.
3. The table refenenced is local to the manifest file, so it *should*
   always have a URI leading with the base prefix of the manifest file.
'''

def manifest_to_template (manifest       : str
                        , template       : str
                        , data_model_uri : str) -> str:
    '''
    Generate an editable template from a turtle manifest
    '''
    logging.debug (f"Called `generate_manifest_template (manifest = {manifest}, template = {template}, data_model_uri = {data_model_uri})'")
    py_data_model_view = SchemaView (data_model_uri)

    loader = RDFLibLoader ()
    dumper = YAMLDumper   ()

    logging.info ("Loading manifest")
    staging_manifest = loader.load (source       = manifest
                                  , target_class = ManifestDesc
                                  , schemaview   = py_data_model_view)
    
    logging.info (f"Dumping manifest to {template}")
    dumper.dump (staging_manifest, template)

    return (template)
    
def template_to_manifest (template       : str
                        , manifest       : str
                        , data_model_uri : str
                        , prefixes       : dict[str,str]) -> bool:
    '''
    Generate a turtle manifest from an editable template

    While the LinkML validation functions + command-line tools don't seem
    to trip up up on duplicate identifiers, the conversion scripts do. In
    this function, this would concern the call to the loader's `load()'
    method.

    Unfortunately, while the conversion script will accept keys *without
    a prefix* as different from keys *with a prefix*, it considers the
    former to be equivalent to an identifier with the prefix, but does
    not throw an error!

    For example, a table description with the identifier (`atomic_name')
    `sampling' is equivalent to `saved:sampling', and this will be
    serialised back to turtle silently dropping all duplicates excepting
    the first.
    '''
    logging.debug (f"Called `template_to_manifest (manifest = {manifest}, template = {template}, data_model_uri = {data_model_uri})'")
    
    py_data_model_view  = SchemaView (data_model_uri)
        
    loader = YAMLLoader   ()
    dumper = RDFLibDumper ()

    logging.info ("Loading template file")
    staging_template = loader.load (source       = template
                                      , target_class = ManifestDesc)

    logging.info (f"Dumping template to {manifest}")
    dumper.dump (staging_template, manifest
                 , schemaview = py_data_model_view
                 , prefix_map = prefixes)

def cli () -> None:
    print (f"This is fisjob version {__version__}")

    op_to_yaml   = ["manifest-to-template", "to-template", "from-manifest"]
    op_to_turtle = ["template-to-manifest", "to-manifest", "from-template"]
    
    parser = argparse.ArgumentParser ("fisjob")
    verbgr = parser.add_mutually_exclusive_group (required = False)
    
    parser.add_argument ("mode"
                       , type = str
                       , choices = op_to_yaml + op_to_turtle
                       , help = "Select mode: conversion from RDF/TTL job manifest to editable YAML template, or vice versa")
    parser.add_argument ("input"
                       , help = "Conversion input (must exist)"
                         , type = str)
    parser.add_argument ("output"
                       , help = "Conversion output (will not overwrite by default)"
                         , type = str)
    parser.add_argument ("--data-model-uri"
                       , help     = "Data model YAML specification URI"
                       , default  = "https://marine.gov.scot/metadata/saved/schema/meta.yaml")
    parser.add_argument ("--force", "-F"
                       , help = "If output file exists, overwrite it"
                       , action = "store_true")
    parser.add_argument ("--base-prefix"
                       , help    = "@base prefix from which job manifest, job results, data and descriptive statistics may be served."
                       , default = "https://marine.gov.scot/metadata/saved/rap/")
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

    if (args.mode in op_to_yaml):        
        print (f"Converting RDF/TTL job manifest {args.input} to editable YAML template {args.output}")

        if (not (isfile (args.input))):
            print (f"Input RDF/TTL job manifest {args.input} does not exist!")
        elif (isfile (args.output) and not (args.force)):
            print (f"Output editable YAML template {args.output} already exists. Overwrite by passing the -f flag.")
        else:
            res_fp = manifest_to_template (manifest       = args.input
                                         , template       = args.output
                                         , data_model_uri = args.data_model_uri)
            
            print (f"Converted RDF/TTL job manifest {args.input} to editable YAML template {args.output}")

    # No need to check for the other options as `argparse' errors
    else:
        print (f"Converting editable YAML template {args.input} to RDF/TTL job manifest {args.output}")
        
        if (not (isfile (args.input))):
            print (f"Input editable YAML template {args.input} does not exist!")
        elif (isfile (args.output) and not (args.force)):
            print (f"Output RDF/TTL manifest {args.output} already exists. Overwrite by passing the -f flag.")
        else:
            prefixes = { "_base": args.base_prefix
                       , "rap"  : "https://marine.gov.scot/metadata/saved/rap/" 
                       , "saved": "https://marine.gov.scot/metadata/saved/schema/"}

            res_bool = template_to_manifest (template       = args.input
                                           , manifest       = args.output
                                           , data_model_uri = args.data_model_uri
                                           , prefixes       = prefixes)
            if (res_bool):
                print (f"Converted editable YAML template {args.input} to RDF/TTL job manifest {args.output}")        
