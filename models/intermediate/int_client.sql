WITH temp_rank AS(
    SELECT *, ROW_NUMBER() OVER(PARTITION BY CLIENT_ID ORDER BY REQUEST_AT) row_n
    FROM {f ref('base_box_requests') }}
)

SELECT
    CLIENT_ID
    CLIENT ADDRESS,
    CLIENT_CITY,
    PHONE,
    CLIENT_NAME
FROM temp_rank
WHERE row_n = 1