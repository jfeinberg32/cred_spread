{% test assert_no_negative_oas(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}