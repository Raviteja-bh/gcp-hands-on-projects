import pytest
import json
import copy
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import sys
import os

# Ensures the runner can find your local pipeline modules
sys.path.append(os.path.join(os.getcwd(), "cat-risk-pipeline/pipeline"))

from dataflow_pipeline import (
    ParseEventFn,
    ValidateEventFn,
    EnrichEventFn
)

VALID_EVENT = {
    "source": "USGS",
    "event_type": "earthquake",
    "magnitude": 5.2,
    "location": "Southern California",
    "timestamp": "2026-05-08T10:00:00"
}

class TestDataflowLogic:
    def test_parse_valid_json(self):
        """Verifies raw bytes are correctly converted to a dict"""
        with TestPipeline() as p:
            input_data = [json.dumps(VALID_EVENT).encode("utf-8")]
            output = (p | beam.Create(input_data) | beam.ParDo(ParseEventFn()))
            assert_that(output, equal_to([VALID_EVENT]))

    def test_enrich_adds_risk_score(self):
        """Verifies the enrichment logic we implemented"""
        with TestPipeline() as p:
            output = (p | beam.Create([copy.deepcopy(VALID_EVENT)]) | beam.ParDo(EnrichEventFn()))
            
            def check_enrichment(elements):
                assert "risk_score" in elements[0]
                assert "processed_at" in elements[0]
            assert_that(output, check_enrichment)

    def test_api_connectivity(self):
        """Integration test for USGS connectivity"""
        import requests
        res = requests.get("https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&limit=1")
        assert res.status_code == 200