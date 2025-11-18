{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        {# In production, use just the custom schema name (bronze, silver, gold) #}
        {{ custom_schema_name | trim }}

    {%- else -%}
        {# In dev, use dev_username_schemaname to avoid conflicts #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
