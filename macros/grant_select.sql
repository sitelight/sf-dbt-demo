{% macro grant_select(role) %}
    {% if target.name == 'prod' %}
        grant select on {{ this }} to role {{ role }};
    {% endif %}
{% endmacro %}
