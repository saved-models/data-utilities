from fisdat.cmd_dat import manifest_wrapper
from fisdat.cmd_up import convert_feasibility, coalesce_schema, coalesce_manifest

import logging
from pathlib import Path, PurePath
import os
from shutil import copytree, rmtree, ignore_patterns
import unittest

logging_format = "%(levelname)s [%(asctime)s] [`%(filename)s\' `%(funcName)s\' (l.%(lineno)d)] ``%(message)s\'\'"
logging_level  = logging.DEBUG
        
data_model_uri    = "https://marine.gov.scot/metadata/saved/schema/meta.yaml"
data_model_uri_ne = "https://marine.gov.scot/metadata/saved/schema/.test.yaml"

prefixes          = { "_base": "https://marine.gov.scot/metadata/saved/rap/"
                    , "rap":   "https://marine.gov.scot/metadata/saved/rap/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
prefixes_alt      = { "_base": "https://marine.gov.scot/metadata/saved/rap_alt/"
                    , "rap":   "https://marine.gov.scot/metadata/saved/rap/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
prefixes_alt_alt  = { "_base": "https://marine.gov.scot/metadata/saved/rap_alt_alt/"
                    , "rap":   "https://marine.gov.scot/metadata/saved/rap/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }

data0    = PurePath ("examples/sentinel_cages/sentinel_cages_cleaned.csv")
data1    = PurePath ("examples/sentinel_cages/Sentinel_cage_station_info_6.csv")
data_ne  = PurePath ("examples/sentinel_cages/.cagedata.csv")
data_bad = PurePath ("examples/sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv")

schema_yaml0   = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.yaml")
schema_yaml0_c = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.converted.yaml")
schema_yaml1   = PurePath ("examples/sentinel_cages/sentinel_cages_site.yaml")
schema_yaml1_c = PurePath ("examples/sentinel_cages/sentinel_cages_site.converted.yaml")
schema_yaml_ne = PurePath ("examples/sentinel_cages/.sampling.yaml")

schema_ttl0    = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.ttl")
schema_ttl0_c  = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.converted.ttl")
schema_ttl1    = PurePath ("examples/sentinel_cages/sentinel_cages_site.ttl")
schema_ttl1_c  = PurePath ("examples/sentinel_cages/sentinel_cages_site.converted.ttl")
schema_ttl_ne  = PurePath ("examples/sentinel_cages/.sampling.ttl")

class TestFeasibility (unittest.TestCase):

    '''
    Conversion feasibility

    Case 1: Known-good data, target does not exist      -> feasible
    Case 2: Input extension equivalent to target's      -> not feasible
    Case 3: Target file already exists                  -> not feasible
    Case 4: Target file already exists, force-overwrite -> feasible
    
    '''
    def test_conv_feasibility0 (self):
        print ("Conversion/moving feasibility case 1: Known-good data, target does not exist")
        res0 = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.jsonld")
        res2 = PurePath ("examples/sentinel_cages/sentinel_cages_site.jsonld")
        res3 = PurePath ("/tmp/sentinel_cages_sampling.jsonld")

        (test_signal0, target_path0) = convert_feasibility (
            input_path  = schema_yaml0
          , target_ext  = "jsonld"
          , force       = False
        )
        (test_signal1, target_path1) = convert_feasibility (
            input_path  = schema_yaml0
          , target_path = res0
          , target_ext  = "jsonld"
          , force       = False
        )
        (test_signal2, target_path2) = convert_feasibility (
            input_path  = schema_yaml1
          , target_path = res2
          , target_ext  = "jsonld"
          , force       = False
        )
        (test_signal3, target_path3) = convert_feasibility (
            input_path  = schema_yaml0
          , target_path = res3
          , target_ext  = "jsonld"
          , force       = False
        )
        self.assertTrue (all ([test_signal0, test_signal1, test_signal2
                             , target_path0 == res0, target_path1 == res0
                             , target_path2 == res2, target_path3 == res3]))

    def test_conv_feasibility1 (self):
        print ("Conversion/moving feasibility case 2: Input extension equivalent to target's")
        (test_signal, target_path) = convert_feasibility (
            input_path = schema_yaml0
          , target_ext = "yaml"
          , force      = False
        )
        self.assertTrue (not test_signal and target_path == schema_yaml0)

    def test_conv_feasibility2 (self):
        print ("Conversion/moving feasibility case 3: Target file already exists, forcibly overwrite")
        (test_signal, target_path) = convert_feasibility (
            input_path = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.json")
          , target_ext = "yaml"
          , force      = False
        )
        self.assertTrue (not test_signal and target_path == schema_yaml0)

    def test_conv_feasibility3 (self):
        print ("Conversion/moving feasibility case 4: Target file already exists, forcibly overwrite")
        (test_signal, target_path) = convert_feasibility (
            input_path = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.json")
          , target_ext = "yaml"
          , force      = True
        )
        self.assertTrue (test_signal and target_path == schema_yaml0)

class TestConvertSchema (unittest.TestCase):
    '''
    Conversion of schemata

    Case 1: YAML to TTL conversion (should succeed)
    Case 2: TTL to YAML conversion (should fail, we don't care about this)
    Case 3: Conversion with --dry-run option (should succeed even if invalid)
    '''
    def test_schema0 (self):
        print ("Schema conversion case 1: YAML to TTL should be feasible")

        res = "/tmp/sentinel_cages_sampling0.ttl"
        
        (test_signal, target_path) = coalesce_schema (
            schema_path_yaml = str(schema_yaml0)
          , schema_path_ttl  = res
          , dry_run          = False
          , force            = False
        )
        try:
            self.assertTrue (test_signal and target_path == PurePath (res))
            os.remove (res)
        except FileNotFoundError as e:
            self.assertFalse (bool(e))

    def test_schema1 (self):
        print ("Schema conversion case 2: TTL to YAML conversion should error")

        res_ttl  = "/tmp/sentinel_cages_sampling1.ttl"
        res_yaml = "/tmp/sentinel_cages_sampling1.yaml"

        (test_signal_ttl, target_path_ttl) = coalesce_schema (
            schema_path_yaml = str(schema_yaml0)
          , schema_path_ttl  = res_ttl
          , dry_run          = False
          , force            = False
        )
        (test_signal_yaml, target_path_yaml) = coalesce_schema (
            schema_path_yaml = str(target_path_ttl)
          , schema_path_ttl  = res_yaml
          , dry_run          = False
          , force            = False
        )
        try:
            self.assertTrue (all ([test_signal_ttl, target_path_ttl == PurePath(res_ttl), not test_signal_yaml]))
            os.remove (res_ttl)
        except FileNotFoundError as e:
            self.assertFalse (bool(e))

    def test_schema2 (self):
        print ("Schema conversion case 3: Imfeasible conversion, but set `dry-run'")
        
        res = "/tmp/sentinel_cages_sampling2.ttl"
        
        (test_signal, target_path) = coalesce_schema (
            schema_path_yaml = str(schema_yaml0)
          , schema_path_ttl  = res
          , dry_run          = True
          , force            = False
        )
        print (test_signal, target_path)
        print (type (target_path))
        print (target_path == PurePath (res))
        
        try:
            self.assertTrue (test_signal and target_path == PurePath (res))
            os.remove (res)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))


def gen_test_manifest (self, message, n
                     , ontology_uri #,tmpcwd
                     , resource_step_0, schema_step_0, schema_step_0_converted, manifest_format_step_0
                     , resource_step_1, schema_step_1, schema_step_1_converted, manifest_format_step_1
                     , manifest_format_out, force_out, manifest_format_converted
                     , validate0, validate1, validate_out, dry_run_out
                     , expected0, expected1, expected_out, fs_op = True):

    output_name        = f"LeafManifest{n}"
    output_uri         = f"https://marine.gov.scot/metadata/saved/rap/LeafManifest{n}"
    
    #if (manifest_format_out == "ttl"):
    #    output_manifest = f"/tmp/manifest{n}.annotated.ttl"
    #else:
    #    output_manifest = f"/tmp/manifest{n}.{manifest_format_step_0}"
    output_manifest = f"/tmp/manifest{n}.{manifest_format_step_0}"
    converted_manifest = f"/tmp/manifest{n}.converted.{manifest_format_converted}"

    if (manifest_format_out == "ttl"):
        annotated_manifest = f"/tmp/manifest{n}.annotated.ttl"
    else:
        annotated_manifest = output_manifest
    
    print (f"Manifest conversion case {n}: {message}")
    print (f"""For this test:
      Manifest to work on: {output_manifest}
      Manifest to convert to: {converted_manifest}
      Initialisation: Data file {resource_step_0}, input schema file {schema_step_0}, converted schema file {schema_step_0_converted}, manifest working format {manifest_format_step_0}
      Append:         Data file {resource_step_1}, input schema file {schema_step_1}, converted schema file {schema_step_1_converted}, manifest working format {manifest_format_step_1}
      Coalesce format: {manifest_format_out} (force: {force_out})
      Validate on manifest initialisation: {validate0}
      Validate on manifest append: {validate1}
      Validate/convert table schema: {validate_out}
      Simulate manifest conversion: {dry_run_out}
      Expected results: Initialisation: {expected0}; Append: {expected1}; Conversion: {expected_out}
    """)
    if (fs_op):
        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))

    test_initialise = manifest_wrapper (
            data            = str(resource_step_0)
          , schema          = str(schema_step_0)
          , data_model_uri  = ontology_uri
          , manifest        = output_manifest
          , manifest_name   = output_name
          , validate        = validate0
          , prefixes        = prefixes
          , serialise_mode  = manifest_format_step_0
    )
    print (f"Manifest conversion case {n}: Initialise result: {test_initialise}")
    test_append = manifest_wrapper (
            data            = str(resource_step_1)
          , schema          = str(schema_step_1)
          , data_model_uri  = ontology_uri
          , manifest        = output_manifest
          , manifest_name   = output_name
          , validate        = validate1
          , prefixes        = prefixes
          , serialise_mode  = manifest_format_step_1
    )
    print (f"Manifest conversion case {n}: Append result: {test_append}")
    (test_signal, test_obj, test_path_yaml, test_path_ttl, test_uri) = coalesce_manifest (
            manifest_path   = output_manifest
          , manifest_format = manifest_format_out
          , data_model_uri  = ontology_uri
          , prefixes        = prefixes
          , gcp_source      = None
          , dry_run         = dry_run_out
          , convert_schema  = validate_out 
          , force           = force_out
          , fake_cwd        = "/tmp/examples/sentinel_cages/"
    )
    print (f"Manifest conversion case {n}: Coalesce result: {test_signal}")
    
    if test_obj is None:
        print (f"Manifest conversion case {n}: returned object None, remaining fields not filled out")
        test_results = [expected0 == test_initialise, expected1 == test_append
                      , not test_signal
                      , test_obj is None, test_path_yaml is None, test_path_ttl is None, test_uri is None]
        try:
            print (test_results)
            self.assertTrue (all (test_results))
            os.remove (output_manifest)
            rmtree ("/tmp/examples/sentinel_cages")
        except FileNotFoundError as e:
            print ("Could not remove files, try removing /tmp/examples and run tests again")
            self.assertFalse (bool(e))
    
    else:
        print (f"Manifest conversion case {n}, I found: {test_path_yaml}, {test_path_ttl}, {test_uri}")
        [table0] = list (filter (lambda k : k.schema_path_yaml == schema_step_0.name, test_obj.tables))
        [table1] = list (filter (lambda k : k.schema_path_yaml == schema_step_1.name, test_obj.tables))

        print (f"Manifest conversion case {n}, I found tables:")
        print (table0)
        print (table1)
        
        # Fairly annoying:
        if (manifest_format_converted == "ttl"):
            print (f"Manifest conversion case {n}, cf {output_manifest}, {converted_manifest}")
            test_fmt = test_path_yaml == PurePath (output_manifest) and test_path_ttl == PurePath (converted_manifest)
        elif (manifest_format_converted == "yaml"):
            print (f"Manifest conversion case {n}, cf {converted_manifest}, {annotated_manifest}")
            test_fmt = test_path_yaml == PurePath (converted_manifest) and test_path_ttl == PurePath (annotated_manifest)
        else:
            test_fmt = False

        print (f"SCHEMA_STEP_0: {schema_step_0.name}, SCHEMA_STEP_1: {schema_step_1.name}, SCHEMA_STEP_0_CONVERTED: {schema_step_0_converted.name}, SCHEMA_STEP_1_CONVERTED: {schema_step_1_converted.name}")
        print (f"T0YML: {table0.schema_path_yaml}, T1YML: {table1.schema_path_yaml}, T0TTL: {table0.schema_path_ttl}, T1TTL: {table1.schema_path_ttl}")
        if (validate_out and expected_out):
            test_tables = all ([
                table0.schema_path_yaml == schema_step_0.name
              , table1.schema_path_yaml == schema_step_1.name
              , table0.schema_path_ttl  == schema_step_0_converted.name
              , table1.schema_path_ttl  == schema_step_1_converted.name
            ])
        else:
            test_tables = all ([
                table0.schema_path_yaml == schema_step_0.name
              , table1.schema_path_yaml == schema_step_1.name
              , table0.schema_path_ttl  is None
              , table1.schema_path_ttl  is None
            ])
        
        test_results = [expected0 == test_initialise
                      , expected1 == test_append
                      , expected_out  == test_signal
                      , test_uri == output_uri
                      , test_fmt, test_tables ]
        
        try:
            print (test_results)
            self.assertTrue (all (test_results))
            if (test_initialise):
                print (f"Removing initially composed manifest {output_manifest}")
                os.remove (output_manifest)
            if (test_signal and not dry_run_out):
                print (f"Removing converted manifest {converted_manifest}")
                os.remove (converted_manifest)
                if (manifest_format_out == "ttl"):
                    print (f"Removing annotated TTL manifest {annotated_manifest}")
                    os.remove (annotated_manifest)
            if (fs_op):
                print ("Removing temporary example directory /tmp/examples/sentinel_cages")
                rmtree ("/tmp/examples/sentinel_cages")
            
        except FileNotFoundError as e:
            print ("Could not remove files, try removing /tmp/examples and run tests again")
            self.assertFalse (bool(e))    

class TestConvertManifest (unittest.TestCase):
    '''
    Case 1: Build up known-good YAML data with cmd_dat      -> (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
    Case 2: Build up known-good TTL data with cmd_dat       -> as in (1)
    Case 3: Manifest path does not exist                    -> (False, None, ...)
    Case 4: Data model URI is invalid                       -> (False, None, ...)
    Case 5: Try loading YAML with "ttl" `manifest_format'   -> (False, None, ...)
    Case 6: Try loading TTL with "yaml" `manifest_format'   -> (False, None, ...)
    Case 7: Some invalid `manifest_format' e.g. "jsonld"    -> (False, None, ...)
    Case 8: Conversion to TTL not feasible (e.g. paths)     -> as in (1), (2), but signal is False
    Case 9: Conversion to TTL not feasible, but force:=True -> as in (1), (2)
    
    Case 10: Build manifest with validation disabled, load with validation -> as in (1, 2), but signal is False
    Case 11: As in (9), but disable validation when loading                -> as in (1), (2)    
    '''

    def test_manifest00 (self):
        gen_test_manifest (
            self, message = "Build up known-good YAML data", n=0
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = True
        )

    def test_manifest01 (self):
        gen_test_manifest (
            self, message = "Build up known-good TTL data", n=1
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "ttl", manifest_format_step_1 = "ttl", manifest_format_out = "ttl", force_out = False, manifest_format_converted = "yaml"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = True
        )

    def test_manifest02 (self):
        print ("Manifest conversion case 3: Non-existent manifest file path")
        (test_signal, test_obj, test_path_yaml, test_path_ttl, test_uri) = coalesce_manifest (
            manifest_path   = "/tmp/manifest2.ttl"
          , manifest_format = "ttl"
          , data_model_uri  = data_model_uri
          , prefixes        = prefixes
          , gcp_source      = None
          , dry_run         = False
          , force           = False
          , fake_cwd        = "/tmp/examples/sentinel_cages/"
        )
        self.assertTrue (all ([not test_signal, test_obj is None, test_path_yaml is None, test_path_ttl is None, test_uri is None]))

    def test_manifest03 (self):
        print ("Manifest conversion case 4: Non-extant data model URI")
        res = "/tmp/manifest3.yaml"
        Path (res).touch ()
        (test_signal, test_obj, test_path_yaml, test_path_ttl, test_uri) = coalesce_manifest (
            manifest_path   = res
          , manifest_format = "yaml"
          , data_model_uri  = data_model_uri_ne
          , prefixes        = prefixes
          , gcp_source      = None
          , dry_run         = False
          , force           = False
          , fake_cwd        = "/tmp/examples/sentinel_cages/"
        )
        #print ((test_signal, test_obj, test_path_yaml, test_path_ttl, test_uri))
        try:
            os.remove (res)
            self.assertTrue (all ([not test_signal, test_obj is None, test_path_yaml is None, test_path_ttl is None, test_uri is None]))
        except FileNotFoundError as e:
            print ("Could not remove files, try removing /tmp/examples and run tests again")
            self.assertFalse (bool(e))

    def test_manifest04 (self):
        gen_test_manifest (
            self, message = "Try loading YAML manifest using TTL loader", n=4
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "ttl", force_out = False, manifest_format_converted = "yaml"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = False
        )

    def test_manifest05 (self):
        gen_test_manifest (
            self, message = "Try loading TTL manifest using YAML loader", n=5
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "ttl", manifest_format_step_1 = "ttl", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = False
        )

    def test_manifest06 (self):
        gen_test_manifest (
            self, message = "Try loading with invalid loader format ('jsonld')", n=6
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "ttl", manifest_format_step_1 = "ttl", manifest_format_out = "jsonld", force_out = False, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = False
        )

    def test_manifest07 (self):
        gen_test_manifest (
            self, message = "Test disabling schema validation/conversion returns no TTL conversion, but success converting manifest proper", n=7
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = False, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = True
        )

    #    def test_manifest08 (self):
    #        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
    #        extant_schema = "/tmp/examples/sentinel_cages/sentinel_cages_sampling.converted.ttl"
    #        Path (extant_schema).touch()
    #        gen_test_manifest (
    #            self, message = "Attempt schema validation/conversion, but at least one schema file already exists", n=8, fs_op=False
    #            , ontology_uri = data_model_uri
    #            , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
    #            , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
    #            , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
    #            , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
    #            , expected0 = True, expected1 = True, expected_out = False
    #        )
    #        rmtree ("/tmp/examples/sentinel_cages")
    #        
    #
    #    def test_manifest09 (self):
    #        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
    #        extant_schema = "/tmp/examples/sentinel_cages/sentinel_cages_sampling.converted.ttl"
    #        Path (extant_schema).touch()
    #        gen_test_manifest (
    #            self, message = "Attempt schema validation/conversion, but at least one schema file already exists, so disable validation", n=9, fs_op=False
    #            , ontology_uri = data_model_uri
    #            , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
    #            , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
    #            , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
    #            , validate0 = True, validate1 = True, validate_out = False, dry_run_out = False
    #            , expected0 = True, expected1 = True, expected_out = True
    #        )
    #        rmtree ("/tmp/examples/sentinel_cages")
        
    def test_manifest10 (self):
        extant = "/tmp/manifest10.converted.ttl"
        Path (extant).touch ()
        gen_test_manifest (
            self, message = "Converted manifest target file already exists", n=10
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = False
        )
        os.remove (extant)

    def test_manifest11 (self):
        extant = "/tmp/manifest11.converted.ttl"
        Path (extant).touch ()
        gen_test_manifest (
            self, message = "Converted manifest target file already exists, forcibly overwrite", n=11
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = True, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
          , expected0 = True, expected1 = True, expected_out = True
        )

    def test_manifest12 (self):
        gen_test_manifest (
            self, message = "Build up known-good YAML data, dry run", n=12
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = True
          , expected0 = True, expected1 = True, expected_out = True
        )
        
    def test_manifest13 (self):
        gen_test_manifest (
            self, message = "Build up known-good TTL data, dry run", n=13
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "ttl", manifest_format_step_1 = "ttl", manifest_format_out = "ttl", force_out = False, manifest_format_converted = "yaml"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = True
          , expected0 = True, expected1 = True, expected_out = True
        )

    def test_manifest14 (self):
        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
        extant_schema0_converted = "/tmp/examples/sentinel_cages/sentinel_cages_sampling.converted.ttl"
        extant_schema1_converted = "/tmp/examples/sentinel_cages/sentinel_cages_site.converted.ttl"
        extant_manifest_converted = "/tmp/manifest14.converted.yaml"
        Path (extant_schema0_converted).touch  ()
        Path (extant_schema1_converted).touch  ()
        Path (extant_manifest_converted).touch ()

        gen_test_manifest (
        self, message = "Build up known-good TTL data, paths already exist, dry run", n=14, fs_op = False
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "ttl", manifest_format_step_1 = "ttl", manifest_format_out = "ttl", force_out = False, manifest_format_converted = "yaml"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = True
          , expected0 = True, expected1 = True, expected_out = False
        )
        with open (extant_schema0_converted, "r") as fp0:
            data = fp0.read ()
            self.assertTrue (len (data) == 0)
        with open (extant_schema1_converted, "r") as fp1:
            data = fp1.read ()
            self.assertTrue (len (data) == 0)
        with open (extant_manifest_converted, "r") as fp2:
            data = fp2.read ()
            self.assertTrue (len (data) == 0)
        os.remove (extant_manifest_converted)
        rmtree ("/tmp/examples/sentinel_cages")
            
    def test_manifest15 (self):
        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
        extant_schema0_converted = "/tmp/examples/sentinel_cages/sentinel_cages_sampling.converted.ttl"
        extant_schema1_converted = "/tmp/examples/sentinel_cages/sentinel_cages_site.converted.ttl"
        extant_manifest_converted = "/tmp/manifest15.converted.yaml"
        Path (extant_schema0_converted).touch  ()
        Path (extant_schema1_converted).touch  ()
        Path (extant_manifest_converted).touch ()

        gen_test_manifest (
        self, message = "Build up known-good TTL data, paths already exist, dry run, would forcibly overwrite", n=15, fs_op = False
          , ontology_uri = data_model_uri
          , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
          , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
          , manifest_format_step_0 = "ttl", manifest_format_step_1 = "ttl", manifest_format_out = "ttl", force_out = True, manifest_format_converted = "yaml"
          , validate0 = True, validate1 = True, validate_out = True, dry_run_out = True
          , expected0 = True, expected1 = True, expected_out = True
        )
        with open (extant_schema0_converted, "r") as fp0:
            data = fp0.read ()
            self.assertTrue (len (data) == 0)
        with open (extant_schema1_converted, "r") as fp1:
            data = fp1.read ()
            self.assertTrue (len (data) == 0)
        with open (extant_manifest_converted, "r") as fp2:
            data = fp2.read ()
            self.assertTrue (len (data) == 0)
        os.remove (extant_manifest_converted)
        rmtree ("/tmp/examples/sentinel_cages")

    #    def test_manifest16 (self):
    #        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
    #        extant_schema_converted = "/tmp/examples/sentinel_cages/sentinel_cages_sampling.converted.ttl"
    #        Path (extant_schema_converted).touch()
    #        gen_test_manifest (
    #            self, message = "Attempt schema validation/conversion, but at least one schema file already exists, dry run", n=16, fs_op=False
    #            , ontology_uri = data_model_uri
    #            , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
    #            , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
    #            , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
    #            , validate0 = True, validate1 = True, validate_out = True, dry_run_out = False
    #            , expected0 = True, expected1 = True, expected_out = False
    #        )
    #        with open (extant_schema_converted, "r") as fp0:
    #            data = fp0.read ()
    #            self.assertTrue (len (data) == 0)
    #        rmtree ("/tmp/examples/sentinel_cages")

    #    def test_manifest17 (self):
    #        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
    #        extant_schema_converted = "/tmp/examples/sentinel_cages/sentinel_cages_sampling.converted.ttl"
    #        Path (extant_schema_converted).touch()
    #        gen_test_manifest (
    #            self, message = "Attempt schema validation/conversion, but at least one schema file already exists, so disable validation, dry run", n=17, fs_op=False
    #            , ontology_uri = data_model_uri
    #            , resource_step_0 = data0, schema_step_0 = schema_yaml0, schema_step_0_converted = schema_ttl0_c
    #            , resource_step_1 = data1, schema_step_1 = schema_yaml1, schema_step_1_converted = schema_ttl1_c
    #            , manifest_format_step_0 = "yaml", manifest_format_step_1 = "yaml", manifest_format_out = "yaml", force_out = False, manifest_format_converted = "ttl"
    #            , validate0 = True, validate1 = True, validate_out = False, dry_run_out = False
    #            , expected0 = True, expected1 = True, expected_out = True
    #        )
    #        with open (extant_schema_converted, "r") as fp0:
    #            data = fp0.read ()
    #            self.assertTrue (len (data) == 0)
    #        rmtree ("/tmp/examples/sentinel_cages")
