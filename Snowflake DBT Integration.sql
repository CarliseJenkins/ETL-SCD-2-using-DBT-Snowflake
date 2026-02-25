-- generate_schema_name.sql --
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%} 

        {{ custom_schema_name }}


    {%- endif -%}

{%- endmacro %}


-- query_tag.sql --

{% macro set_query_tag() -%}

  {% set new_query_tag = model.name %} {# always use model name #}

  {% if new_query_tag %}

    {% set original_query_tag = get_current_query_tag() %}

    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}

    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}

    {{ return(original_query_tag)}}

  {% endif %}

  -- packages.yml. --

  packages:

  - package: dbt-labs/dbt_utils

    version: 0.8.0

  {{ return(none)}}

{% endmacro %}


-- copy_into_snowflake.sql -- 

{% macro macros_copy_csv(table_nm) %} 

delete from {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};
 
COPY INTO {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 
FROM 
(
SELECT

    $1 AS ProductId,

    $2 AS ProductName,

    $3 AS Category,

    $4 AS SellingPrice,

    $5 AS ModelNumber,

    $6 AS AboutProduct,

    $7 AS ProductSpecification,

    $8 AS TechnicalDetails,

    $9 AS ShippingWeight,

    $10 AS ProductDimensions,

    CURRENT_TIMESTAMP() AS INSERT_DTS,

    CURRENT_TIMESTAMP() AS UPDATE_DTS,

    metadata$filename AS SOURCE_FILE_NAME,

    metadata$file_row_number AS SOURCE_FILE_ROW_NUMBER

FROM @{{ var('stage_name') }}
)
FILE_FORMAT = {{var ('file_format_json') }}
PURGE={{ var('purge_status') }}
FORCE = TRUE;
{% endmacro %}


-- This setting configures which "profile" dbt uses for this project. -- dbt_project.yml --
profile: 'default'
vars:

   wrk_schema: BRONZE

   file_format_json: MYDB.BRONZE.MY_CSV_FORMAT

   purge_status: FALSE

   stage_name: MYDB.BRONZE.MY_S3_STAGE

   rawhist_db: MYDB



 
