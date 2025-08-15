
with source_data as (

    SELECT *
    FROM {{ source('main', 'users') }}
    WHERE is_active = TRUE

)

SELECT name, email, age
FROM source_data
ORDER BY CAST(signup_date AS DATE) ASC
LIMIT 10
