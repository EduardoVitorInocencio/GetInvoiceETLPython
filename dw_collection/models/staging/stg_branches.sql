WITH raw_branches AS (
	SELECT
		*
	FROM
		{{ source('getinvoice', 'getinvoice') }}
)
SELECT 
    DISTINCT "Branch" AS branch,
    CASE
        WHEN "Branch" = '133' THEN 'São Paulo'
        WHEN "Branch" = '150' THEN 'Rio de Janeiro'
        WHEN "Branch" = '209' THEN 'Alphaville'
        WHEN "Branch" = '252' THEN 'Curitiba'
        ELSE 'NÃO IDENTIFICADO'
    END AS states,
    'Brasil' AS country
FROM raw_branches
ORDER BY "Branch"