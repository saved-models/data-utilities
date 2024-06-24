from fisdat.cmd_up import convert_feasibility, coalesce_schema

import logging
from pathlib import PurePath
import os
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

manifest_yaml = "/tmp/manifest.yaml"
manifest_ttl  = "/tmp/manifest.ttl"
manifest_name = "RootManifest"

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

class TestConversion (unittest.TestCase):
    '''
    Conversion of schemata

    Case 1: YAML to TTL conversion (should succeed)
    Case 2: TTL to YAML conversion (should fail, we don't care about this)
    '''
    def test_schema0 (self):
        print ("Schema conversion case 1: YAML to TTL should be feasible")

        res = PurePath ("/tmp/sentinel_cages_sampling0.ttl")
        
        (test_signal, target_path) = coalesce_schema (
            schema_path_yaml = schema_yaml0
          , schema_path_ttl  = res
          , dry_run          = False
          , force            = False
        )
        try:
            os.remove (res)
            self.assertTrue (test_signal and target_path == res)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))

    def test_schema1 (self):
        print ("Schema conversion case 2: TTL to YAML conversion should error")

        res_ttl  = PurePath ("/tmp/sentinel_cages_sampling1.ttl")
        res_yaml = PurePath ("/tmp/sentinel_cages_sampling1.yaml")

        (test_signal_ttl, target_path_ttl) = coalesce_schema (
            schema_path_yaml = schema_yaml0
          , schema_path_ttl  = res_ttl
          , dry_run          = False
          , force            = False
        )
        (test_signal_yaml, target_path_yaml) = coalesce_schema (
            schema_path_yaml = target_path_ttl
          , schema_path_ttl  = res_yaml
          , dry_run          = False
          , force            = False
        )
        try:
            os.remove (res_ttl)
            self.assertTrue (all ([test_signal_ttl, target_path_ttl == res_ttl, not test_signal_yaml]))
        except FileNotFoundError as e:
            self.assertTrue (bool(e))
