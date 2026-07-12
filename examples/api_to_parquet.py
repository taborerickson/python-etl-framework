# examples/api_to_parquet.py 

"""
examples/api_to_parquet.py 

End-to-end example: extract -> transform -> load. 

Pulls posts from JSONPlaceholder (a free, no-auth-required public test API), 
passes themthrough a no-op transformer, and writes them to a local Parquet 
file. Run with: 

    python examples/api_to_parquet.py 
"""

import os 

from etl_framework import (
    configure_logging, 
    APIConfig, 
    RestApiExtractor, 
    PassThroughTransformer, 
    ParquetLoader, 
)

def main(): 
    """
    Wires together and runs the full extract -> transform -> load pipeline. 

    Builds an APIConfig for the JSONPlaceholder /posts endpoint, passes the 
    extracted records through PassThroughTransformer unchanged, and writes 
    the results to output/posts.parquet via ParquetLoader. 

    Nothing in the pipeline actually executes until laoder.run() begins 
    iterating. extractor.run() and transformer.transform() are both 
    generators, so records are pulled from the API, transformed, and 
    written to disk one batch at a time rather than all at once. 
    """
    configure_logging(level="INFO", environment="development")

    config = APIConfig(
        source_name="jsonplaceholder_posts", 
        pipeline_run_id="example-run-001", 
        url="https://jsonplaceholder.typicode.com/posts", 
        auth_token="not-required-for-this-api", 
        timeout_seconds=10, 
    )

    extractor = RestApiExtractor(config) 
    transformer = PassThroughTransformer() 

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True) 
    output_path = os.path.join(output_dir, "posts.parquet") 
    loader = ParquetLoader(output_path=output_path, batch_size=20) 

    raw_records = extractor.run() 
    transformed_records = transformer.transform(raw_records) 
    written = loader.run(transformed_records) 

    print(f"Wrote {written} records to {output_path}") 


if __name__ == "__main__": 
    main() 

