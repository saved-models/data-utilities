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
            os.remove (res)
            self.assertTrue (test_signal and target_path == PurePath (res))
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
            os.remove (res_ttl)
            self.assertTrue (all ([test_signal_ttl, target_path_ttl == PurePath(res_ttl), not test_signal_yaml]))
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
        try:
            os.remove (res)
            self.assertTrue (test_signal and target_path == PurePath (res))
        except FileNotFoundError as e:
            self.assertFalse (bool(e))


def gen_test_manifest (self, message, n
                     , man_in, man_conv
                     , ont #,tmpcwd
                     , res0, sch0, sch0conv, man_fmt0
                     , res1, sch1, sch1conv, man_fmt1
                     , man_fmt_out, force_out, man_fmt_conv
                     , val0, val1, dry_run_out
                     , exp0, exp1, exp_out):
    print (f"Manifest conversion case {n+1}: {message}")
    print (f"""For this test:
      Manifest to work on: {man_in}
      Manifest to convert to: {man_conv}
      Initialisation: Data file {res0}, input schema file {sch0}, converted schema file {sch0conv}, manifest working format {man_fmt0}
      Append:         Data file {res1}, input schema file {sch1}, converted schema file {sch1conv}, manifest working format {man_fmt1}
      Coalesce format: {man_fmt_out} (force: {force_out}), manifest convert to {man_conv}
      Validate on init: {val0}
      Validate on output: {val1}
      Simulate manifest conversion: {dry_run_out}
      Expected results: Initialisation: {exp0}; Append: {exp1}; Conversion: {exp_out}
    """)
    copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))

    output_name        = f"LeafManifest{n}"
    output_uri         = f"https://marine.gov.scot/metadata/saved/rap/LeafManifest{n}"
    output_manifest    = f"/tmp/manifest{n}.{man_fmt0}"
    converted_manifest = f"/tmp/manifest{n}.converted.{man_fmt_conv}"

    test_initialise = manifest_wrapper (
            data = str(res0)
            , schema = str(sch0)
            , data_model_uri = ont
            , manifest = output_manifest
            , manifest_name = output_name
            , validate = val0
            , prefixes = prefixes
            , serialise_mode = man_fmt0
    )
    print (f"Manifest conversion case {n}: Initialise result: {test_initialise}")
    test_append = manifest_wrapper (
            data = str(res1)
            , schema = str(sch1)
            , data_model_uri = ont
            , manifest = output_manifest
            , manifest_name = output_name
            , validate = val1
            , prefixes = prefixes
            , serialise_mode = man_fmt1
    )
    print (f"Manifest conversion case {n}: Append result: {test_append}")
    (test_signal, test_obj, test_path_yaml, test_path_ttl, test_uri) = coalesce_manifest (
            manifest_path   = output_manifest
            , manifest_format = man_fmt_out
            , data_model_uri  = ont
            , prefixes        = prefixes
            , gcp_source      = None
            , dry_run         = dry_run_out
            , force           = force_out
            , fake_cwd        = "/tmp/examples/sentinel_cages/"
    )
    print (f"Manifest conversion case {n}: Coalesce result: {test_signal}")
    
    
    if test_obj is None:
        print (f"Manifest conversion case {n}: returned object None, remaining fields not filled out")
        test_results = [exp0 == test_initialise, exp1 == test_append
                      , not test_signal
                      , test_obj is None, test_path_yaml is None, test_path_ttl is None, test_uri is None]
        try:
            print (test_results)
            os.remove (output_manifest)
            rmtree ("/tmp/examples/sentinel_cages")
            self.assertTrue (all (test_results))
        except FileNotFoundError as e:
            print ("Could not remove files, try removing /tmp/examples and run tests again")
            self.assertFalse (bool(e))
    
    else:
        print (f"Manifest conversion case {n}, I found: {test_path_yaml}, {test_path_ttl}, {test_uri}")
        [table0] = list (filter (lambda k : k.schema_path_yaml == sch0.name, test_obj.tables))
        [table1] = list (filter (lambda k : k.schema_path_yaml == sch1.name, test_obj.tables))

        print (f"Manifest conversion case {n}, I found tables:")
        print (table0)
        print (table1)
        
        # Fairly annoying:
        if (man_fmt_conv == "ttl"):
            test_fmt = test_path_yaml == PurePath (man_in) and test_path_ttl == PurePath (man_conv)
        elif (man_fmt_conv == "yaml"):
            test_fmt = test_path_yaml == PurePath (man_conv) and test_path_ttl == PurePath (man_in)
        else:
            test_fmt = False

        print (f"SCH0: {sch0.name}, SCH1: {sch1.name}, SCH0CONV: {sch0conv.name}, SCH1CONV: {sch1conv.name}")
        test_tables = all([
                table0.schema_path_yaml == sch0.name
              , table1.schema_path_yaml == sch1.name
              , table0.schema_path_ttl  == sch0conv.name
              , table1.schema_path_ttl  == sch1conv.name
            ])
        
        test_results = [exp0 == test_initialise
                      , exp1 == test_append
                      , exp_out  == test_signal
                      , test_uri == output_uri
                      , test_fmt, test_tables ]
        
        try:
            print (test_results)
            if (exp0 == test_initialise):
                os.remove (output_manifest)
            if (exp_out == test_signal):
                os.remove (converted_manifest)
            rmtree ("/tmp/examples/sentinel_cages")   
            self.assertTrue (all (test_results))
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

    def test_manifest0 (self):
        gen_test_manifest (
            self, message = "Build up known-good YAML data", n=0
          , man_in   = "/tmp/manifest0.yaml"
          , man_conv = "/tmp/manifest0.converted.ttl"
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "yaml", man_fmt1 = "yaml", man_fmt_out = "yaml", force_out = False, man_fmt_conv = "ttl"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = True
        )

    def test_manifest1 (self):
        gen_test_manifest (
            self, message = "Build up known-good TTL data", n=1
          , man_in   = "/tmp/manifest1.ttl"
          , man_conv = "/tmp/manifest1.converted.yaml"
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "ttl", man_fmt1 = "ttl", man_fmt_out = "ttl", force_out = False, man_fmt_conv = "yaml"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = True
        )

    def test_manifest2 (self):
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

    def test_manifest3 (self):
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

    def test_manifest4 (self):
        gen_test_manifest (
            self, message = "Try loading YAML manifest using TTL loader", n=4
          , man_in   = "/tmp/manifest4.yaml"
          , man_conv = "/tmp/manifest4.converted.ttl"
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "yaml", man_fmt1 = "yaml", man_fmt_out = "ttl", force_out = False, man_fmt_conv = "yaml"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = False
        )

    def test_manifest5 (self):
        gen_test_manifest (
            self, message = "Try loading TTL manifest using YAML loader", n=5
          , man_in   = "/tmp/manifest5.ttl"
          , man_conv = "/tmp/manifest5.converted.yaml"
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "ttl", man_fmt1 = "ttl", man_fmt_out = "yaml", force_out = False, man_fmt_conv = "ttl"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = False
        )

    def test_manifest6 (self):
        gen_test_manifest (
            self, message = "Try loading with invalid loader format ('jsonld')", n=6
          , man_in   = "/tmp/manifest6.ttl"
          , man_conv = "/tmp/manifest6.converted.yaml"
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "ttl", man_fmt1 = "ttl", man_fmt_out = "jsonld", force_out = False, man_fmt_conv = "ttl"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = False
        )

    def test_manifest7 (self):
        extant = "/tmp/manifest7.converted.ttl"
        Path (extant).touch ()
        gen_test_manifest (
            self, message = "Converted manifest target file already exists", n=7
          , man_in   = "/tmp/manifest7.yaml"
          , man_conv = extant
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "yaml", man_fmt1 = "yaml", man_fmt_out = "yaml", force_out = False, man_fmt_conv = "ttl"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = False
        )

    def test_manifest8 (self):
        extant = "/tmp/manifest8.converted.ttl"
        Path (extant).touch ()
        gen_test_manifest (
            self, message = "Converted manifest target file already exists, forcibly overwrite", n=8
          , man_in   = "/tmp/manifest8.yaml"
          , man_conv = extant
          , ont = data_model_uri
          , res0 = data0, sch0 = schema_yaml0, sch0conv = schema_ttl0_c
          , res1 = data1, sch1 = schema_yaml1, sch1conv = schema_ttl1_c
          , man_fmt0 = "yaml", man_fmt1 = "yaml", man_fmt_out = "yaml", force_out = True, man_fmt_conv = "ttl"
          , val0 = True, val1 = True, dry_run_out = False
          , exp0 = True, exp1 = True, exp_out = True
        )
        
