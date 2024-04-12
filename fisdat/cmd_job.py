import argparse
from os.path import isfile
import logging

from linkml.generators.pythongen import PythonGenerator
from linkml_runtime.loaders      import RDFLibLoader, YAMLLoader
from linkml_runtime.dumpers      import YAMLDumper  , RDFLibDumper

from fisdat import __version__, __commit__
from fisdat.utils import validation_helper
from importlib import resources as ir
from . import data_model as dm

def manifest_to_template (manifest   : str
                        , template   : str
                        , data_model : str) -> str:
    '''
    Generate an editable template from a turtle manifest
    '''
    logging.debug (f"Called `generate_manifest_template (manifest = {manifest}, template = {template}, data_model = {data_model})'")

    # PythonGenerator seems to spit out WARNING messages regardless of log_level
    logging.info (f"Creating Python object from data model {data_model}")
    py_data_model_base = PythonGenerator (data_model)

    logging.info (f"Compiling Python object")
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview

    target_class = py_data_model_module.ManifestDesc

    loader = RDFLibLoader ()
    dumper = YAMLDumper   ()

    logging.info ("Loading manifest")
    staging_manifest = loader.load (source       = manifest
                                  , target_class = target_class
                                  , schemaview = py_data_model_view)
    
    logging.info (f"Dumping manifest to {template}")
    dumper.dump (staging_manifest, template)

    return (template)
    
def template_to_manifest (template   : str
                        , manifest   : str
                        , data_model : str) -> bool:
    '''
    Generate a turtle manifest from an editable template
    '''
    logging.debug (f"Called `template_to_manifest (manifest = {manifest}, template = {template}, data_model = {data_model})'")
    validation_test = validation_helper (data = template, schema = data_model, target_class = "ManifestDesc")
    
    if (validation_test):
        # PythonGenerator seems to spit out WARNING messages regardless of log_level
        logging.info (f"Creating Python object from data model {data_model}")
        py_data_model_base = PythonGenerator (data_model)

        logging.info (f"Compiling Python object")
        py_data_model_module = py_data_model_base.compile_module ()
        py_data_model_view   = py_data_model_base.schemaview

        target_class = py_data_model_module.ManifestDesc
        
        loader = YAMLLoader   ()
        dumper = RDFLibDumper ()

        logging.info ("Loading template file")
        staging_template = loader.load (source = template
                                      , target_class = target_class)

        logging.info (f"Dumping template to {manifest}")
        dumper.dump (staging_template, manifest, schemaview = py_data_model_view)
    return (validation_test)

def cli () -> None:
    print (f"This is fisjob version {__version__}, commit {__commit__}")
    
    parser = argparse.ArgumentParser ("fisjob")
    verbgr = parser.add_mutually_exclusive_group (required = False)
    
    parser.add_argument ("mode"
                       , type = str
                       , choices = ["manifest-to-template", "to-template"
                                  , "template-to-manifest", "to-manifest"]
                       , help = "Select mode: conversion from RDF/TTL job manifest to editable YAML template, or vice versa")
    parser.add_argument ("input"
                       , help = "Conversion input (must exist)"
                         , type = str)
    parser.add_argument ("output"
                       , help = "Conversion output (will not overwrite by default)"
                         , type = str)
    parser.add_argument ("--data-model"
                       , help    = "Data model YAML specification in fisdat/data_model/src/model"
                       , default = "job")
    parser.add_argument ("--force", "-f"
                       , help = "If output file exists, overwrite it"
                       , action = "store_true")
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

    logging.debug (f"Polling data model directory")
    root_dir = ir.files (dm)
    logging.debug (f"Data model working directory is: {root_dir}")
    yaml_sch = f"src/model/{args.data_model}.yaml"
    logging.debug (f"Data model path is: {yaml_sch}")
    
    data_model_path = root_dir / yaml_sch
    data_model      = str (data_model_path)
    
    if (args.mode == "manifest-to-template" or args.mode == "to-template"):
        
        print (f"Converting RDF/TTL job manifest {args.input} to editable YAML template {args.output}")

        if (not (isfile (args.input))):
            print (f"Input RDF/TTL job manifest {args.input} does not exist!")
        elif (isfile (args.output) and not (args.force)):
            print (f"Output editable YAML template {args.output} already exists. Overwrite by passing the -f flag.")
        else:
            res_fp = manifest_to_template (manifest   = args.input
                                         , template   = args.output
                                         , data_model = data_model)

            print (f"Converted RDF/TTL job manifest {args.input} to editable YAML template {res_fp}")
            
    else:
        print (f"Converting editable YAML template {args.input} to RDF/TTL job manifest {args.output}")
        
        if (not (isfile (args.input))):
            print (f"Input editable YAML template {args.input} does not exist!")
        elif (isfile (args.output) and not (args.force)):
            print (f"Output RDF/TTL manifest {args.output} already exists. Overwrite by passing the -f flag.")
        else:
            res_bool = template_to_manifest (template   = args.input
                                           , manifest   = args.output
                                           , data_model = data_model)
            if (res_bool):
                print (f"Converted RDF/TTL job manifest {args.input} to editable YAML template {args.input}")
        