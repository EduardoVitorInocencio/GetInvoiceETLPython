WITH raw_resellers AS (
	SELECT
		public.getinvoice."Reseller Number" 	AS reseller_number,
		public.getinvoice."Reseller Name"		AS reseller_name,
		public.getinvoice."Reseller Area"		AS reseller_area,
		ROW_NUMBER()OVER(PARTITION BY "Reseller Number" ORDER BY "Reseller Number") AS row_num
	FROM
		{{ source('getinvoice', 'getinvoice') }}
)
SELECT
	reseller_number,
	reseller_name,
	reseller_area
FROM 
	raw_resellers 
WHERE row_num = 1;