WITH raw_brokers AS (
	SELECT
		public.getinvoice."Broker Area" AS broker_area,
		public.getinvoice."Broker Name" AS broker_name,
		public.getinvoice."Broker Number" AS broker_number,
		ROW_NUMBER()OVER(PARTITION BY "Broker Number" ORDER BY "Broker Number") AS row_num
	FROM 
		{{ source('getinvoice', 'getinvoice') }}
)
SELECT
	broker_area,
	broker_name,
	broker_number
FROM raw_brokers 
WHERE row_num = 1;