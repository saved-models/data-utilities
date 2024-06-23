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
        
data0 = "examples/sentinel_cages/sentinel_cages_cleaned.csv"
data1 = "examples/sentinel_cages/Sentinel_cage_station_info_6.csv"
data_ne   = "examples/sentinel_cages/.cagedata.csv"
data_bad  = "examples/sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv"

schema0 = "examples/sentinel_cages/sentinel_cages_sampling.yaml"
schema1  = "examples/sentinel_cages/sentinel_cages_site.yaml"
schema_ne   = "examples/sentinel_cages/.sampling.yaml"


class TestAppend (unittest.TestCase):
    '''
    Case 1: YAML manifest init, YAML manifest append -> True
    Case 2: TTl manifest init, TTL manifest append   -> True
    Case 3: YAML manifest init, TTL manifest append  -> False
    Case 4: TTL manifest init, YAML manifest append  -> False
    '''

    def test_manifest_append0 (self):
        print ("Test case #1: YAML manifest init, YAML manifest append")

        test_initialise = manifest_wrapper (
            data           = data0
          , schema         = schema0
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/append0.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        test_append = manifest_wrapper (
            data           = data1
          , schema         = schema1
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/append0.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        try:
            os.remove ("/tmp/append0.yaml")
            self.assertTrue (test_initialise and test_append)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))

    def test_manifest_append1 (self):
        print ("Test case #2: TTL manifest init, TTL manifest append")

        test_initialise = manifest_wrapper (
            data           = data0
          , schema         = schema0
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/append1.ttl"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "ttl"
        )
        test_append = manifest_wrapper (
            data           = data1
          , schema         = schema1
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/append1.ttl"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "ttl"
        )
        try:
            os.remove ("/tmp/append1.ttl")
            self.assertTrue (test_initialise and test_append)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))

    def test_manifest_append2 (self):
        print ("Test case #3: YAML manifest init, TTL manifest append")

        test_initialise = manifest_wrapper (
            data           = data0
          , schema         = schema0
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/append2.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        test_append = manifest_wrapper (
            data           = data1
          , schema         = schema1
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/append2.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "ttl"
        )
        try:
            os.remove ("/tmp/append2.yaml")
            self.assertFalse (test_initialise and not test_append)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))
    
class TestInit (unittest.TestCase):
    '''
    Case 1: Known-good test data                                -> True
    Case 2: Neither data nor YAML schema exist                  -> False
    Case 3: Data exists, but YAML schema does not               -> False
    Case 4: Data does not exist, but YAML schema does           -> False
    Case 5: Data/schema exist, but fail validation              -> False
    Case 6: Data/schema exist, would fail [disabled] validation -> True

    Testing just the initialisation code here also lets us first test
    the file existence/validation code, as this is called regardless
    of the serialisation mode (YAML/TTL) or append mode (initialise
    or append). For the initialisation stage, the actual format doesn't
    matter and should be tested elsewhere.
    '''

    def test_manifest_init0 (self):
        print ("Test case #1: Known-good test data")
        
        test = manifest_wrapper (
            data           = data0
          , schema         = schema0
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/initialise0.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        try:
            os.remove ("/tmp/initialise0.yaml")
            self.assertTrue (test)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))
    
    def test_manifest_init1 (self):
        print ("Test case #2: Neither data nor YAML schema exist")
        
        test = manifest_wrapper (
            data           = data_ne
          , schema         = schema_ne
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/initialise1.yaml"
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
          , manifest       = "/tmp/initialise2.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        test1 = manifest_wrapper (
            data           = data_ne
          , schema         = schema0
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/initialise3.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        self.assertFalse (test0 or test1)

    def test_manifest_init3 (self):
        print ("Test case #4: Data/schema exist, but fail validation")

        test = manifest_wrapper (
            data           = data0
          , schema         = schema1
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/initialise4.yaml"
          , manifest_name  = manifest_name
          , validate       = True
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        self.assertFalse (test)

    def test_manifest_init5 (self):
        print ("Test case #6: Data/schema exist, would fail [disabled] validation")

        test = manifest_wrapper (
            data           = data0
          , schema         = schema1
          , data_model_uri = data_model_uri
          , manifest       = "/tmp/initialise5.yaml"
          , manifest_name  = manifest_name
          , validate       = False
          , prefixes       = prefixes
          , serialise_mode = "yaml"
        )
        try:
            os.remove ("/tmp/initialise5.yaml")
            self.assertTrue (test)
        except FileNotFoundError as e:
            self.assertTrue (bool(e))
