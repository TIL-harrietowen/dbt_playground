{#
    --let's develop a macro that
    1. queries the information schema of a database
    2. finds objects that are >1 year old (no longer maintained) - normally recmomend 1 week
    3. generates automated drop statements
    4. has the ability to execute those drop statements
#}

{% macro clean_stale_models(database=target.database, schema=target.schema, dry_run=True) %}

    {% set get_drop_commands_query %}
        select
            case
                when table_type='VIEW' THEN table_type
                else 'TABLE'
            end as drop_object,
            'DROP ' || drop_object || ' {{ database | upper }}.' || table_schema || '.' || table_name as drop_statement

        from {{ database }}.information_schema.tables
        where table_schema = upper('{{ schema }}')
        order by last_altered desc

    {% endset%}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}
    
    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query) %}
        {% endif %}
    {% endfor %}

{% endmacro %}