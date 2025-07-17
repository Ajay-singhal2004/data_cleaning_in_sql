-- ###data cleaning

-- 1.ramove duplicates
-- 2.standardize the data
-- 3.null value or blank values
-- 4.remove any column

create database world_layoffs;

-- #create a copy of data to

create table layoffs_staging
like layoffs;

insert layoffs_staging
(select * from layoffs);

select * from layoffs_staging;


-- 2remove duplicates work on table layoffs_staging

with  cte_duplicate as
(select * ,row_number() 
over(partition by company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions ) as row_no
from layoffs_staging)
select * from cte_duplicate
where row_no >1;

-- create table with row_no

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_no` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

-- insert date into layoffs_staging2

insert  layoffs_staging2
(select * ,row_number() 
over(partition by company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions ) as row_no
from layoffs_staging);

select distinct (company),trim(company)
 from layoffs_staging2
 where row_no >1;
 
 delete from layoffs_staging2
 where row_no>1;
 
 set sql_safe_updates=0;


-- #3 standerdize data


select distinct company ,trim(company)
from layoffs_staging2;


update layoffs_staging2
set company= trim(company);


select * from layoffs_staging2;

select distinct(industry)
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry= 'crypto'
where industry like 'crypto%';

select * from layoffs_staging2
where industry like 'cryp%';


select distinct (country) ,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'united%';

select `date`,str_to_date(`date`,'%m/%d/%Y')
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date`=str_to_date(`date`,'%m/%d/%Y');
 
 select * from layoffs_staging2;
 
 select total_laid_off,percentage_laid_off from layoffs_staging2;
 
select * from layoffs_staging2
where industry is null
or industry ='';

select * from layoffs_staging2
where company = 'airbnb';


select * 
from layoffs_staging2 as t1
join layoffs_staging2 as t2
on t1.company=t2.company
and t1.location=t2.location
where (t1.industry is null or t1.industry='')
and  t2.industry is not null;

select  t1.industry,t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
on t1.company=t2.company
and t1.location=t2.location
where ( t1.industry is null or t1.industry=''
and t2.industry is not null);

update layoffs_staging2
set industry=null
where industry='';


update layoffs_staging2 as t1
join layoffs_staging2 as t2
on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null
and t2.industry is not null);

select * from layoffs_staging2;


-- drop column

alter table layoffs_staging2
drop column row_no;








