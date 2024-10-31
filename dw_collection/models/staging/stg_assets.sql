WITH raw_assets AS (
	SELECT
		public.getinvoice."Asset no" AS asset_number,
		public.getinvoice."Asset type" AS asset_type,
		public.getinvoice."Asset" AS asset,
		public.getinvoice."Manufacturer" AS manufacturer,
		ROW_NUMBER()OVER(PARTITION BY "Asset no" ORDER BY "Asset no") AS row_num
	FROM
		{{ source('getinvoice', 'getinvoice') }}
)
SELECT
	asset_number,
	asset_type,
	asset,
	manufacturer
FROM
	raw_assets
WHERE
	row_num = 1;
