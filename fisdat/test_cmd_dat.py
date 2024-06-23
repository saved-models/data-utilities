from fisdat.cmd_dat import manifest_wrapper, append_job_manifest

import logging
import os
import unittest

logging_format = "%(levelname)s [%(asctime)s] [`%(filename)s\' `%(funcName)s\' (l.%(lineno)d)] ``%(message)s\'\'"
logging_level  = logging.DEBUG
        
data_model_uri = "https://marine.gov.scot/metadata/saved/schema/meta.yaml"
manifest_yaml  = "/tmp/manifest.yaml"
manifest_ttl   = "/tmp/manifest.ttl"
manifest_name  = "RootManifest"
prefixes = { "_base": "https://marine.gov.scot/metadata/saved/rap/"
           , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
        
data_good = "examples/sentinel_cages/sentinel_cages_cleaned.csv"
data_ne   = "examples/sentinel_cages/.cagedata.csv"
data_bad  = "examples/sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv"

schema_good = "examples/sentinel_cages/sentinel_cages_sampling.yaml"
schema_ne   = "examples/sentinel_cages/.sampling.yaml"
schema_bad  = "examples/sentinel_cages/sentinel_cages_site.yaml"

class TestInit (unittest.TestCase):
    '''
    Case 1: Known-good test data                                -> True
    Case 2: Neither data nor YAML schema exist                  -> False
    Case 3: Data exists, but YAML schema does not               -> False
    Case 4: Data does not exist, but YAML schema does           -> False
    Case 5: Data/schema exist, but fail validation              -> False
    Case 6: Data/schema exist, would fail [disabled] validation -> True
    '''

    def test_manifest_init0 (self):
        print ("Test case #1: Known-good test data")
        
        test = manifest_wrapper (
            data           = data_good
          , schema         = schema_good
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test0.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        try:
            os.remove ("/tmp/test0.yaml")
            self.assertTrue (test)
        except FileNotFoundError as e:
            self.assertTrue (bool (e))
    
    def test_manifest_init1 (self):
        print ("Test case #2: Neither data nor YAML schema exist")
        
        test = manifest_wrapper (
            data           = data_ne
          , schema         = schema_ne
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test1.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        self.assertFalse (test)

    def test_manifest_init2 (self):
        print ("Test case #3: Data exists, but YAML schema does not, or vice versa")
        
        test0 = manifest_wrapper (
            data           = data_ne
          , schema         = schema_ne
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test2.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        test1 = manifest_wrapper (
            data           = data_ne
          , schema         = schema_good
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test3.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        self.assertFalse (test0 or test1)

    def test_manifest_init3 (self):
        print ("Test case #4: Data/schema exist, but fail validation")

        test0 = manifest_wrapper (
            data           = data_good
          , schema         = schema_bad
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test4.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        test1 = manifest_wrapper (
            data           = data_bad
          , schema         = schema_good
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test5.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        self.assertFalse (test0 or test1)

    def test_manifest_init5 (self):
        print ("Test case #6: Data/schema exist, would fail [disabled] validation")

        test = manifest_wrapper (
            data           = data_good
          , schema         = schema_bad
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/test6.yaml"
          , manifest_name  = manifest_name
          , validate       = False
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        try:
            os.remove ("/tmp/test6.yaml")
            self.assertTrue (test)
        except FileNotFoundError as e:
            self.assertTrue (bool (e))
