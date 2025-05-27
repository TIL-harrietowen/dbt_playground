{% macro grant_select(schema=target.schema, role=target.role) %}

    {% set sql %}
        grant usage on schema {{ schema }} to role {{ role }};
        grant select on all tables in schema {{ schema }};
        grant select on all views in schema {{ schema }} to role {{ role }};
    {% endset %}

    {{ log('Granting select on all tables and views in schmea ' ~ schema ~ ' to role ' ~ role, info=True) }}
    {# 
        commented out because I don't have permissions in Snowflake to do this
        {% do run_query(sql) %}
        {{ log('Privileges granted', info=True) }} 
    #}

{% endmacro %}