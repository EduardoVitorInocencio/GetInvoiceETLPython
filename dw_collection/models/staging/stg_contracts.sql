WITH raw_contracts AS(
	SELECT
		public.getinvoice."Branch" AS branch,
		public.getinvoice."B+C" AS branch_contract,
		public.getinvoice."Contract Type" AS contract_type,
		public.getinvoice."Start Of Lease Term" AS start_of_lease_term,
		public.getinvoice."Duration" AS duration,
		public.getinvoice."Termination" AS termination,
		public.getinvoice."Insurance Key" AS insurance_key,
		public.getinvoice."NAV" AS nav,
		public.getinvoice."Instalment" AS installment,
		public.getinvoice."Broker Number" AS broker_number,
		public.getinvoice."Asset no" AS asset_number,
		ROW_NUMBER()OVER(PARTITION BY "B+C" ORDER BY "B+C") AS row_num
	FROM 
		{{ source('getinvoice', 'getinvoice') }}	
)
SELECT
	branch,
	branch_contract,
	contract_type,
	start_of_lease_term,
	duration,
	termination,
	insurance_key,
	nav,
	installment,
	broker_number,
	asset_number
FROM
	raw_contracts
WHERE row_num = 1;