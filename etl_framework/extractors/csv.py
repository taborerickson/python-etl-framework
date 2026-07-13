# extractors/csv.py 

"""
CSV Extractor 

Concrete extractor for pulling data from a local CSV file. 
Implements BaseExtractor's abstract extract() method as a generator that 
streams rows one at a time via csv.DictReader, rather than loading the full 
file into memory.  

Maps file-level and parsing failures to the framework's exception hierarchy: 
    - Missing file                          -> SourceNotFoundError (permanent)
    - Decode/encoding errors                -> MalformedFileError (permanent) 
    - Malformed CSV structure               -> MalformedFileError (permanent) 

Retry design: 
    Unlike RestApiExtractor, there are no eager/retryable fetches here. A local 
    file read failing is (almost always) a permanent condition: the file doesn't 
    exist, or its contents are corrupt. Retrying a FileNotFoundError does not fix 
    anything the way retrying a network timeout might. So this extractor deliberately
    wraps nothing with the retry() decorator. Retry is not a mandatory part of 
    BaseExtractor's contract.  
"""

import csv 

from etl_framework.base.extractor import BaseExtractor 
from etl_framework.config.models import CSVConfig 
from etl_framework.exceptions.pipeline_errors import (
    SourceNotFoundError,  
    MalformedFileError, 
)

# CSVExtractor 
class CSVExtractor(BaseExtractor): 
    """Extracts records from a local CSV file, one row at a time."""

    def __init__(self, config: CSVConfig) -> None: 
        super().__init__(config) 

    def extract(self): 
        """
        Generator. Yields one dict per CSV row via csv.DictReader. 

        Nothing in this method executes until the generator is iterated, including
        opening the file. A CSVExtractor can be constructed against a nonexistent 
        path with no error; the SourceNotFoundError only fires when you start pulling
        records from it. 

        Raises: 
            SourceNotFoundError: if config.file_path does not exist. 
            MalformedFileError: on decode errors or malformed CSV structure. 
        """
        try: 
            with open(
                self.config.file_path, 
                encoding=self.config.encoding, 
                newline="", 
            ) as f: 
                reader = csv.DictReader(f, delimiter=self.config.delimiter) 
                yield from reader 
        except FileNotFoundError as e: 
                raise SourceNotFoundError(
                    f"CSV file not found: {self.config.file_path}"
                ) from e 
        except UnicodeDecodeError as e: 
                raise MalformedFileError(
                     f"Could not decode {self.config.file_path} as "
                     f"{self.config.encoding}"
                ) from e 
        except csv.Error as e: 
                raise MalformedFileError(
                     f"Malformed CSV structure in {self.config.file_path}: {e}"
                ) from e 
           

