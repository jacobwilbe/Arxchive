CREATE DATABASE ARXIV_RAG;
CREATE SCHEMA ARXIV_DATA

create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd


class text_chunker:
    def process(self, pdf_text: str):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512,
            chunk_overlap = 256,
            length_function = len
        )

        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])

        yield from df.itertuples(index=False, name=None)
$$;

create or replace stage research encryption = (type = 'SNOWFLAKE_SSE') directory = (enable = true);

create or replace TABLE RESEARCH_CHUNKS_TABLE (
        RELATIVE_PATH VARCHAR(16777216), 
        SIZE NUMBER(38, 0),
        CHUNK VARCHAR(16777216)
    );


create or replace CORTEX SEARCH SERVICE ARXIV_SEARCH_SERVICE
    ON chunk
    ATTRIBUTES relative_path
    warehouse = COMPUTE_WH
    TARGET_LAG = '1 minute'
    as (
        select chunk,
            relative_path
        from research_chunks_table
    );


create or replace stream research_stream on stage research;



CREATE OR REPLACE TASK refresh_stage_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute' 
AS
ALTER STAGE research REFRESH;


CREATE OR REPLACE TASK parse_and_insert_pdf_task
WAREHOUSE = COMPUTE_WH
AFTER refresh_stage_task 
WHEN SYSTEM$STREAM_HAS_DATA('research_stream') 
AS
INSERT INTO research_chunks_table (relative_path, size, chunk)
SELECT 
    relative_path,
    size,
    func.chunk AS chunk
FROM 
    research_stream,
    TABLE(text_chunker(TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@research, relative_path, {'mode': 'LAYOUT'})))) AS func;

alter task refresh_stage_task resume;
alter task parse_and_insert_pdf_task resume;



