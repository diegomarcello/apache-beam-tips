import argparse
import logging
import os
import re
import uuid

import apache_beam as beam
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore

from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.types import Key
from apache_beam.io.gcp.datastore.v1new.types import Query
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class EntityWrapper(object):
  """Create a Cloud Datastore entity from the given string."""
  def __init__(self, project, namespace, kind):
    self._project = project
    self._namespace = namespace
    self._kind = kind

  def make_entity(self, content):
    
    # Namespace and project are inherited from parent key.
    key = Key([self._kind, str(uuid.uuid4())])
    entity = Entity(key)
    entity.set_properties(content)
    return entity



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    
    PROJECT = "octadata-octalink-prd-351822"
    
    
    pipeline_options = {
        'project': PROJECT,
        'staging_location': 'gs://' + BUCKET + '/staging',
        'region': 'us-central1',
        'runner': 'DataflowRunner',
        'job_name': JOB_NAME,
        'disk_size_gb': 100,
        'temp_location': 'gs://' + BUCKET + '/staging',
        'save_main_session': True,
        'requirements_file': 'requirements.txt'
    }


    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        bq_filter_rows = (p | "READ_BQ" >> beam.io.ReadFromBigQuery(
            project=PROJECT,
            dataset="RAW_FIRESTORE_SAMPLE",
            table='CATALOGOS', 
            method="DIRECT_READ"
            )
    # | "MAP" >> beam.Map(print)
    
    | 'create entity' >> beam.Map(
        EntityWrapper(
            PROJECT,
            "data_firestore",
            "catalogos_3").make_entity)
    | "IMPORT" >> WriteToDatastore(
        project=PROJECT, 
        throttle_rampup=True, 
        hint_num_workers=10
        )
    )

    # lines = p | 'Read' >> ReadFromText(known_args.input)

    # output = (
    #   lines
    #   | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
    #   | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
    #   | 'GroupAndSum' >> beam.CombinePerKey(sum)
    #   # For Logging Purposes
    #   | 'Format' >> beam.MapTuple(format_output))

    # # A custom text sink so it displays nicely in GCS :(
    # output | 'Write' >> Write(Utf8TextSink(known_args.output))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()