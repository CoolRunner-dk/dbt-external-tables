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
    BEGIN
        SELECT NULL;
    END;

    /* Create child master setup of stream and task (1 sub-master per source table) */
    -- Create child stream based on child table.
    -- When created make sure we always run first instance using "SHOW_INITIAL_ROWS = TRUE".
    --  Should not affect actual data inserts as next level of tasks have same setup on their streams.
    --  This to avoid having unprocessed data in next level tasks, since a REPLACE/CREATE starts without data in stream.
    CREATE OR REPLACE STREAM {{ source(source_node.source_name, source_node.name) }}_STR ON TABLE {{ source(source_node.source_name, source_node.name) }} APPEND_ONLY = true SHOW_INITIAL_ROWS = true;
    -- Create child task which only runs when stream on child table has data. Consumes stream when executed.
    CREATE OR REPLACE TASK {{ source(source_node.source_name, source_node.name) }}_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = "Sub Master task for {{ source(source_node.source_name, source_node.name) }}. Is starting point for all tasks dependent on this source table unless overwritten explicitly."
    AFTER {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK
    WHEN -- Only execute if stream has data
    SYSTEM$STREAM_HAS_DATA('{{ source(source_node.source_name, source_node.name) }}_STR')
    AS
    BEGIN
        -- Consume stream to temporary table to reset stream
        CREATE OR REPLACE TEMPORARY TABLE {{ source(source_node.source_name, source_node.name) }}_STR_RESET
        AS
        SELECT *
        FROM {{ source(source_node.source_name, source_node.name) }}_STR
        WHERE 1=2 -- avoid actually storing anything
        ;
    END;

    -- RESUME child master task (since DAG makes sure it doesn't run in dev anyway)
    ALTER TASK {{ source(source_node.source_name, source_node.name) }}_TSK RESUME;

{% endmacro %}
