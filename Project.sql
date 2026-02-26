-- data cleaning--

ALTER TABLE `layoffs`.`raw_data_layoffs` 
RENAME TO  `layoffs`.`layoffs` ;

-- 1. remove duplicates
-- 2. standardize the data
-- 3. hull values or blank values 
-- 4. remove any coloum

CREATE TABLE layoff_dublicate LIKE layoffs;

SELECT *
FROM layoff_dublicate;
    
insert  into layoff_dublicate    
	select *
from layoffs;

SELECT *
FROM
    layoff_dublicate;

-- 1. remove duplicates --

    
CREATE TABLE `layoff_dublicate2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;
;


insert into layoff_dublicate2
 select *,
	row_number() 
		over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
		funds_raised_millions) as row_num
	from layoff_dublicate;
 
SELECT *
FROM
    layoff_dublicate2
WHERE
    row_num > 1;

DELETE FROM layoff_dublicate2 
WHERE
    row_num > 1;
    
-- standardizing data--

SELECT *
FROM
    layoff_dublicate2;
    
     
SELECT company, TRIM(company)
FROM
    layoff_dublicate2;

UPDATE layoff_dublicate2 
SET 
    company = TRIM(company);

SELECT *
FROM
    layoff_dublicate2;

SELECT DISTINCT industry
FROM
    layoff_dublicate2
ORDER BY 1;
    

SELECT *
FROM
    layoff_dublicate2
WHERE
    industry LIKE 'crypto%'
;

UPDATE layoff_dublicate2 
SET 
    industry = 'crypto'
WHERE
    industry LIKE 'crypto%';

SELECT DISTINCT industry
FROM
    layoff_dublicate2
ORDER BY 1;
    
SELECT *
FROM
    layoff_dublicate2;
    
SELECT DISTINCT country
FROM
    layoff_dublicate2
ORDER BY 1;

SELECT DISTINCT country
FROM
    layoff_dublicate2
WHERE
    country LIKE 'united states%';

UPDATE layoff_dublicate2 
SET 
    country = 'united states'
WHERE
    country LIKE 'united states%';

SELECT `date`
FROM
    layoff_dublicate2;
    
UPDATE layoff_dublicate2 
SET 
    `DATE` = STR_TO_DATE(`DATE`, '%m/%d/%Y');

ALTER TABLE layoff_dublicate2
modify column `DATE` date;

-- 3.) NULL VALUES OR BLANK VALUES --




SELECT *
FROM
    layoff_dublicate2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

SELECT REPORT1.industry, REPORT2.industry
FROM
    layoff_dublicate2 REPORT1
        JOIN
    layoff_dublicate2 REPORT2 ON REPORT1.COMPANY = REPORT2.company
WHERE
    (REPORT1.industry = ''
        OR REPORT1.industry IS NULL)
        AND REPORT2.industry IS NOT NULL;


UPDATE layoff_dublicate2 
SET 
    INDUSTRY = NULL
WHERE
    INDUSTRY = '';




UPDATE layoff_dublicate2 REPORT1
        JOIN
    layoff_dublicate2 REPORT2 ON REPORT1.COMPANY = REPORT2.company 
SET 
    REPORT1.INDUSTRY = REPORT2.INDUSTRY
WHERE
    REPORT1.industry IS NULL
        AND REPORT2.industry IS NOT NULL;

SELECT *
FROM
    layoff_dublicate2
WHERE
    industry IS NULL OR INDUSTRY
;


SELECT *
FROM
    layoff_dublicate2;


SELECT *
FROM
    layoff_dublicate2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

DELETE FROM layoff_dublicate2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

SELECT *
FROM
    layoff_dublicate2;

-- 4.) REMOVE ANY COLUMN --

ALTER TABLE layoff_dublicate2
DROP column ROW_NUM;

-- END OF DATA CLEANING -- 

-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- with this info we are just going to look around and see what we find!



SELECT MAX(total_laid_off)
FROM layoff_dublicate2
;






-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoff_dublicate2
WHERE  percentage_laid_off IS NOT NULL
;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoff_dublicate2
WHERE  percentage_laid_off = 1
;
-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM layoff_dublicate2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;
-- BritishVolt looks like an EV company, Quibi! I recognize that company - wow raised like 2 billion dollars and went under - ouch

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM layoff_dublicate2
ORDER BY 2 DESC
LIMIT 5
;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoff_dublicate2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10
;

-- by location
SELECT location, SUM(total_laid_off)
FROM layoff_dublicate2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10
;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM layoff_dublicate2
GROUP BY country
ORDER BY 2 DESC
;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoff_dublicate2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM layoff_dublicate2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoff_dublicate2
GROUP BY stage
ORDER BY 2 DESC;


-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at 

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoff_dublicate2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoff_dublicate2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoff_dublicate2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;


-- DONE--




























