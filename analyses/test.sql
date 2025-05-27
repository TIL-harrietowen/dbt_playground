{% set database = target.database%}
{% set schema = target.schema%}

select
    table_type,
    table_schema,
    table_name,
    last_altered,
    case
        when table_type='VIEW' THEN table_type
        else 'TABLE'
    end as drop_object,
    'DROP ' || drop_object || ' {{ database | upper }}.' || table_schema || '.' || table_name as drop_statement

from {{ database }}.information_schema.tables
where table_schema = upper('{{ schema }}')
order by last_altered desc