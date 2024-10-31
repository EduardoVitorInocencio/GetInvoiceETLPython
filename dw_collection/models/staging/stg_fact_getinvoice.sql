WITH raw_contracts AS(
	SELECT
		public.getinvoice."extractDate" AS date_,
		public.getinvoice."1/ Reminder" AS reminder_1,
		public.getinvoice."2/ Reminder" AS reminder_2,
		public.getinvoice."3/ Reminder" AS reminder_3,
		public.getinvoice."Arrear" AS arrear,
		public.getinvoice."Branch" AS branch,
		public.getinvoice."B+C" AS branch_contract,
		public.getinvoice."Reseller Number" AS reseller_number 
	FROM public.getinvoice	
)
SELECT
	date_,
	reminder_1,
	reminder_2,
	reminder_3,
	arrear,
	branch,
	branch_contract,
	reseller_number
FROM
	raw_contracts;