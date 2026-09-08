SELECT
    my_int,
    animal,
    mojap_file_land_timestamp
FROM {{ database_name }}.table1
WHERE
    {{ primary_partition }} IN ({{ snapshot_timestamps }})
    AND animal = 'chicken'
