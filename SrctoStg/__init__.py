import random
import string
import argparse

class ArgumentParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self._setup_arguments()
        self.etl_batch_id = self._generate_etl_batch_id()
    
    def _setup_arguments(self):
        self.parser.add_argument('-s', '--sources', help="IDs of sources to process, delimited by ','")
        self.parser.add_argument('-g', '--groups', help="Groups (SourceName) to process, delimited by ','")
        self.parser.add_argument('-S', '--exclude-sources', help="Sources to be processed except specified one, delimited by ','")
        self.parser.add_argument('-G', '--exclude-groups', help="Groups to be processed except specified one, delimited by ','")
        self.parser.add_argument('-t', '--object_type', help='Specify object type Dimension or Fact')
        self.parser.add_argument('-i', '--calling_sequence', help='Run only failed items in the Control table')
        self.parser.add_argument('-l', '--loadfrequency', default='Daily', help='Run only failed items in the Control table')
        self.parser.add_argument('-f', '--failed', action='store_true', help='Run only failed items in the Control table')
        self.parser.add_argument('-d', '--delimiter', help='Character used to specify multiple sources in "--sources" switch (default: ,)', default=',')
        self.parser.add_argument('--list_sources', action='store_true', help='List all Source IDs available in the Control Table')
        self.parser.add_argument('-p', '--parallel', action='store_true', help='Spawn separate process for handling each source')
        self.parser.add_argument('-u', '--user_agent', help="User Agent", default='terminal')
    
    def parse_args(self):
        return self.parser.parse_args()
    
    def _generate_etl_batch_id(self):
        return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(11))

