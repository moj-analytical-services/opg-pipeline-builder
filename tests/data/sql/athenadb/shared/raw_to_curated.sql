SELECT
    {{ sql_columns }},
    my_datetime,
    my_date,
    mojap_file_land_timestamp,
    animal
FROM {{ database_name }}.{{ table_name }}
WHERE mojap_file_land_timestamp IN ({{ snapshot_timestamps }})
