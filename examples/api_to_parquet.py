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
    APIConfig,
    ParquetLoader,
    PassThroughTransformer,
    RestApiExtractor,
    configure_logging,
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
        # pipeline_run_id intentionally omitted -- ExtractorConfig now 
        # auto-generates one (timestamp + short suffix) via default_factory. 
        url="https://jsonplaceholder.typicode.com/posts", 
        auth_token="not-required-for-this-api", 
        # JSONPlaceholder ignores page/per_pagee and always returns all 100 posts 
        # in one response. page_size must exceed that count so extract()'s 
        # "short page" stopping condition (len(page) < page_size) actually 
        # triggers on the first request (otherwise it loops forever re-fetching 
        # the same 100 records). 200 is a safe margin above the known count. 
        page_size=200, 
        timeout_seconds=10, 
    )

    # print auto-generated pipeline_run_id for visibility (example pipeline run only)
    print(f"Running pipeline_run_id={config.pipeline_run_id}")

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

