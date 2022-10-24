SELECT
    my_int,
    animal,
    mojap_file_land_timestamp
FROM {{ database_name }}.table1
WHERE {{ primary_partition }} in ({{ snapshot_timestamps }})
    and animal = 'chicken'
