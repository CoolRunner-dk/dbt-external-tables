{% macro snowflake_get_copy_sql(source_node, explicit_transaction=false) %}
{# This assumes you have already created an external stage #}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) %}
    {%- set copy_options = external.snowpipe.get('copy_options', none) -%}

    {%- if explicit_transaction -%} begin; {%- endif %}

    copy into {{source(source_node.source_name, source_node.name)}}
    from (
        select
        {% if columns|length == 0 %}
            $1::variant as value,
        {% else -%}
        {%- for column in columns -%}
            {%- set col_expression -%}
                {%- if is_csv -%}nullif(${{loop.index}},''){# special case: get columns by ordinal position #}
                {%- else -%}nullif($1:"{{column.meta.original_name if column.meta.original_name else column.name}}",''){# standard behavior: get columns by name #}
                {%- endif -%}
            {%- endset -%}
            {{col_expression}}::{{column.data_type}} as {{column.name}},
        {% endfor -%}
        {% endif %}
            metadata$filename as rsrc,
            metadata$file_row_number as file_row_number,
            metadata$start_scan_time as rldts,
            metadata$file_last_modified as pipe_file_last_modified
        from {{external.location}} {# stage #}
    )
    file_format = {{external.file_format}}
    {% if external.pattern -%} pattern = '{{external.pattern}}' {%- endif %}
    {% if copy_options %} {{copy_options}} {% endif %};

    {% if explicit_transaction -%} commit; {%- endif -%}

{% endmacro %}
