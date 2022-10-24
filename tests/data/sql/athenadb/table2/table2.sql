SELECT
    my_int,
    animal,
    {{ primary_partition }}
FROM {{ database_name }}.table1
WHERE {{ primary_partition }} in ({{ snapshot_timestamps }})
    and animal = 'chicken'
