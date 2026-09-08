SELECT
    my_int,
    animal,
    {{ primary_partition }}
FROM {{ database_name }}.table1
WHERE
    {{ primary_partition }} IN ({{ snapshot_timestamps }})
    AND animal = 'chicken'
