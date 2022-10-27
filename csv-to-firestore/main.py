import csv
import datetime
import logging
import apache_beam as beam
from google.cloud import firestore

from apache_beam.options.pipeline_options import PipelineOptions

from settings import PROJECT, BUCKET, INPUT_FILENAME


class FirestoreWriteDoFn(beam.DoFn):
    MAX_DOCUMENTS = 200

    def __init__(self, project, collection):
        self._project = project
        self._collection = collection
        super(FirestoreWriteDoFn, self).__init__()

    def start_bundle(self):
        self._mutations = []

    def finish_bundle(self):
        if self._mutations:
            self._flush_batch()

    def process(self, element, *args, **kwargs):
        self._mutations.append(element)
        if len(self._mutations) > self.MAX_DOCUMENTS:
            self._flush_batch()

    def _flush_batch(self):
        db = firestore.Client(project=self._project)
        batch = db.batch()
        for mutation in self._mutations:
            if len(mutation) == 1:
                # autogenerate document_id
                ref = db.collection(self._collection).document()
                batch.set(ref, mutation)
            else:
                ref = db.collection(self._collection).document(mutation[0])
                batch.set(ref, mutation[1])
        r = batch.commit()
        logging.debug(r)
        self._mutations = []


class CSVtoDict(beam.DoFn):
    """Converts line into dictionary"""

    def process(self, element, headers):
        _element = element.split(',')
        data_element = zip(headers, _element)
        return [dict(data_element)]


class CreateEntities(beam.DoFn):
    """Creates Datastore entity"""

    def process(self, element):
        
        document_id = element.pop('id')
        
        element['location_id'] = int(element['location_id'])
        element['address_1'] = element['address_1']
        element['address_2'] = element['address_2']
        element['city'] = element['city']
        element['state_province'] = element['state_province']
        element['postal_code'] = element['postal_code']
        element['country'] = element['country']
        obj = [(document_id, element)]
        return obj

def dataflow(run_local):

    if run_local:
        input_file_path = '/home/mark_asauske_001/load-firestore/sample.csv'
    else:
        input_file_path = 'gs://' + BUCKET + '/' + INPUT_FILENAME

    JOB_NAME = 'firestore-upload-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

    pipeline_options = {}

    if run_local:
        pipeline_options['runner'] = 'DirectRunner'
    else:
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

    try:
        options = PipelineOptions.from_dictionary(pipeline_options)

        with beam.Pipeline(options=options) as p:
            (p | 'Reading input file' >> beam.io.ReadFromText(input_file_path, skip_header_lines=1)
            | 'Converting from csv to dict' >> beam.ParDo(CSVtoDict(),
                                                        ['id',
                                                        'location_id',
                                                        'address_1',
                                                        'address_2',
                                                        'city',
                                                        'state_province',
                                                        'postal_code',
                                                        'country'])
            | 'Create entities' >> beam.ParDo(CreateEntities())
            | 'Write entities into Firestore' >> beam.ParDo(FirestoreWriteDoFn(PROJECT, 'addresses'))
            )
    except Exception as e:
        print(e)

if __name__ == '__main__':
    run_locally = False
    print("starting execution")
    dataflow(run_locally)