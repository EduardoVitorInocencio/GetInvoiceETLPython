WITH raw_customers AS (
    SELECT
        *
    FROM
        {{ source('getinvoice', 'getinvoice') }}
),
ranked_customer AS(
	SELECT
	raw_customers."Lessee Name" AS lessee_name,
    raw_customers."Customer number" AS customer_number,
    raw_customers."Lessee Street" AS lessee_street,
    raw_customers."Lessee Postal Code" AS lessee_postal_code,
    raw_customers."Lessee Town" AS lessee_town,
    raw_customers."Lessee Area" AS lessee_area,
    raw_customers."Lessee State" AS lessee_state,
    raw_customers."Lessee E-Mail" AS lessee_email,
    raw_customers."Lessee VATID" AS lessee_vatid,
    raw_customers."Serasa Code" AS serasa_score,
    raw_customers."GL Score" AS gl_score,
	
	ROW_NUMBER() OVER(
		PARTITION BY raw_customers."Lessee Name"
		ORDER BY "Lessee Name") AS row_num
	FROM
	    raw_customers
)
SELECT 
	lessee_name,
	customer_number,
	lessee_street,
	lessee_postal_code,
	lessee_town,
	lessee_area,
	lessee_state,
	lessee_email,
	lessee_vatid,
	serasa_score,
	gl_score
FROM 
	ranked_customer 
WHERE 
	row_num =1 
;
