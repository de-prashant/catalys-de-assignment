{% macro generate_hash(columns) %}
    MD5(
        CONCAT_WS(
            '||',
            {% for col in columns %}
                COALESCE(CAST({{ col }} AS STRING), 'NULL')
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
