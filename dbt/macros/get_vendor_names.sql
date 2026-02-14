{% macro get_vendor_names(vendor_id) %}
case
    when {{ vendor_id }} is null then 'Unknown Vendor'
    when {{ vendor_id }} = 1 then 'Creative Mobile Technologies, LLC'
    when {{ vendor_id }} = 2 then 'Verifone Inc.'
    when {{ vendor_id }} = 4 then '4th vendor'
    when {{ vendor_id }} = 6 then '6th vendor'
    else 'other'
end
{% endmacro %}