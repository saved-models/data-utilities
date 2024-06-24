from fisdat.cmd_dat import manifest_wrapper
from fisdat.cmd_up import convert_feasibility, coalesce_schema, coalesce_manifest

import logging
from pathlib import PurePath
import os
from shutil import copytree, rmtree, ignore_patterns
import unittest

'''
Note here that the tests often end in a `try'/`exception' block for `os.remove'.
This is a succint, if rough way of checking that the file exists as expected.
'''

logging_format = "%(levelname)s [%(asctime)s] [`%(filename)s\' `%(funcName)s\' (l.%(lineno)d)] ``%(message)s\'\'"
logging_level  = logging.DEBUG
        
data_model_uri    = "https://marine.gov.scot/metadata/saved/schema/meta.yaml"
data_model_uri_ne = "https://marine.gov.scot/metadata/saved/schema/.test.yaml"

prefixes          = { "_base": "https://marine.gov.scot/metadata/saved/rap/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
prefixes_alt      = { "_base": "https://marine.gov.scot/metadata/saved/rap_alt/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
prefixes_alt_alt  = { "_base": "https://marine.gov.scot/metadata/saved/rap_alt_alt/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }

data0    = PurePath ("examples/sentinel_cages/sentinel_cages_cleaned.csv")
data1    = PurePath ("examples/sentinel_cages/Sentinel_cage_station_info_6.csv")
data_ne  = PurePath ("examples/sentinel_cages/.cagedata.csv")
data_bad = PurePath ("examples/sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv")

schema_yaml0   = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.yaml")
schema_yaml1   = PurePath ("examples/sentinel_cages/sentinel_cages_site.yaml")
schema_yaml_ne = PurePath ("examples/sentinel_cages/.sampling.yaml")

schema_ttl0    = PurePath ("examples/sentinel_cages/sentinel_cages_sampling.ttl")
schema_ttl1    = PurePath ("examples/sentinel_cages/sentinel_cages_site.ttl")
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
            self.assertTrue (bool(e))

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
            self.assertTrue (bool(e))

class TestConvertManifest (unittest.TestCase):
    '''
    Case 1: Build up known-good TTL data with cmd_dat.      -> (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
    Case 2: Build up known-good YAML data with cmd_dat.     -> as in (1)
    Case 2: Manifest path does not exist                    -> (False, None, ...)
    Case 3: Data model URI is invalid                       -> (False, None, ...)
    Case 4: Try loading YAML with "ttl" `manifest_format'   -> (False, None, ...)
    Case 5: Try loading TTL with "yaml" `manifest_format'   -> (False, None, ...)
    Case 6: Some invalid `manifest_format' e.g. "jsonld"    -> (False, None, ...)
    Case 7: Conversion to TTL not feasible (e.g. paths)     -> as in (1), (2), but signal is False
    Case 8: Conversion to TTL not feasible, but force:=True -> as in (1), (2)

    def coalesce_manifest (manifest_path      : str
                     , manifest_format    : str
                     , data_model_uri     : str
                     , prefixes           : dict[str, str]
                     , gcp_source         : str
                     , dry_run            : bool
                     , force              : bool) -> (bool, ManifestDesc, str, str, str, str):
    
    '''

    def test_manifest0 (self):
        copytree ("examples/sentinel_cages", "/tmp/examples/sentinel_cages", ignore = ignore_patterns ("*.ttl"))
        
        test_initialise = manifest_wrapper (
            data           = str(data0)
          , schema         = str(schema_yaml0)
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/manifest0.yaml"
          , manifest_name  = "LeafManifest0"
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        test_append = manifest_wrapper (
            data           = str(data1)
          , schema         = str(schema_yaml1)
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/manifest0.yaml"
          , manifest_name  = "LeafManifest0"
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        (test_signal, test_obj, test_path_yaml, test_path_ttl, test_uri) = coalesce_manifest (
            manifest_path   = "/tmp/manifest0.yaml"
          , manifest_format = "yaml"
          , data_model_uri  = data_model_uri
          , prefixes        = prefixes
          , gcp_source      = None
          , dry_run         = False
          , force           = False
          , fake_cwd        = "/tmp/examples/sentinel_cages/"
        )
        try:
            os.remove ("/tmp/manifest0.yaml")
            os.remove ("/tmp/manifest0.ttl")
            rmtree ("/tmp/examples/sentinel_cages")
            self.assertTrue (all ([test_initialise, test_append, test_signal
                                 , test_path_yaml == PurePath ("/tmp/manifest0.yaml")
                                 , test_path_ttl  == PurePath ("/tmp/manifest0.ttl")
                                 , test_uri       == "https://marine.gov.scot/metadata/saved/rap/LeafManifest0"
                                 , test_obj.tables[0].schema_path_yaml == "sentinel_cages_sampling.yaml"
                                 , test_obj.tables[0].schema_path_ttl  == "sentinel_cages_sampling.ttl"
                                 , test_obj.tables[1].schema_path_yaml == "sentinel_cages_site.yaml"
                                 , test_obj.tables[1].schema_path_ttl  == "sentinel_cages_site.ttl"]))
        except FileNotFoundError as e:
            self.assertTrue (bool(e))
            
        
                             
