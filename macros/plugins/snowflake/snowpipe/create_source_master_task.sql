{% macro snowflake_create_source_master_task(source_node) %}

    -- Create master task if not exists
    CREATE TASK IF NOT EXISTS {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '{{ source_node.source_meta.task_schedule }}'
    {% if target.name == 'prod' %}
    SUSPEND_TASK_AFTER_NUM_FAILURES = 1
    ERROR_INTEGRATION = CR_NI_AWS_ERROR
    {% endif %}
    COMMENT = "Master task for {{ source(source_node.source_name, source_node.name).include(identifier=false) }}. Is starting point for all tasks in schema unless overwritten explicitly."
    AS
    SELECT NULL;

    -- SUSPEND master task before adding a child task
    ALTER TASK {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK SUSPEND;

    /* Create child master setup of stream and task (1 sub-master per source table) */
    -- Create child stream based on child table
    CREATE OR REPLACE STREAM {{ source(source_node.source_name, source_node.name) }}_STR ON TABLE {{ source(source_node.source_name, source_node.name) }} APPEND_ONLY = true;
    -- Create child task which only runs when stream on child table has data. Consumes stream when runned.
    CREATE OR REPLACE TASK {{ source(source_node.source_name, source_node.name) }}_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = "Sub Master task for {{ source(source_node.source_name, source_node.name) }}. Is starting point for all tasks dependent on this source table unless overwritten explicitly."
    AFTER {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK
    WHEN -- Only execute if stream has data
    SYSTEM$STREAM_HAS_DATA('{{ source(source_node.source_name, source_node.name) }}_STR')
    AS
    -- Consume stream to temporary table to reset stream
    CREATE OR REPLACE TEMPORARY TABLE {{ source(source_node.source_name, source_node.name) }}_STR_RESET
    AS
    SELECT *
    FROM {{ source(source_node.source_name, source_node.name) }}_STR
    WHERE 1=2 -- avoid actually storing anything
    ;

    -- RESUME child master task (since DAG makes sure it doesn't run in dev anyway)
    ALTER TASK {{ source(source_node.source_name, source_node.name) }}_TSK RESUME;
    {% if target.name == 'prod' %}
    -- RESUME master task if production
    ALTER TASK {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK RESUME;
    {% endif -%}

{% endmacro %}
