{% macro snowflake_create_source_master_task(source_node) %}

    CREATE TASK IF NOT EXISTS {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '{{ source_node.source_meta.task_schedule }}'
    {% if target.name == 'prod' %}
    SUSPEND_TASK_AFTER_NUM_FAILURES = 1
    ERROR_INTEGRATION = CR_NI_AWS_ERROR
    {% endif %}
    COMMENT = "Master task for {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK. Is starting point for all tasks in schema unless overwritten explicitly."
    AS
    SELECT NULL;

    CREATE OR REPLACE TASK {{ source(source_node.source_name, source_node.name) }}_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    {% if target.name == 'prod' %}
    SUSPEND_TASK_AFTER_NUM_FAILURES = 1
    ERROR_INTEGRATION = CR_NI_AWS_ERROR
    {% endif %}
    COMMENT = "Sub Master task for {{ source(source_node.source_name, source_node.name) }}. Is starting point for all tasks dependent on this source table unless overwritten explicitly."
    AFTER {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK
    AS
    SELECT NULL;

    {% if target.name == 'prod' %}
    -- RESUME tasks if production
    ALTER TASK {{ source(source_node.source_name, source_node.name).include(identifier=false) }}.MASTER_TSK RESUME;
    ALTER TASK {{ source(source_node.source_name, source_node.name) }}_TSK RESUME;
    {% endif -%}

{% endmacro %}
