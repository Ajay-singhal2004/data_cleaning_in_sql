-- exploratory data analysis

select * from layoffs_staging2;


--max and min laid off
select max(total_laid_off),max(percentage_laid_off) 
from layoffs_staging2;


-- 100% laid off
select * 
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions desc;

-- total laid off by company
select company,sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select min(`date`),max(`date`)
from layoffs_staging2;

-- total laid off by industry
select industry ,sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;



select year(`date`),sum(total_laid_off)
from layoffs_staging2 
group by year(`date`)
order by 1 desc;

-- total laid offs in month
select substring(`date`,1,7) as `month`,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
;


with rolling_total as 
(select substring(`date`,1,7) as `month`,sum(total_laid_off)as total
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`,total,
sum(total) over (order by `month`)as rolling_total
from rolling_total;

-- use rolling to find rolling_total
with rolling_total as 
(select substring(`date`,1,7) as `month`,sum(total_laid_off)as total
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`,total,
sum(total) over(order by `month`) as rolling_total
from rolling_total
 ;
 -- laid off by compony in spacific year
 select company,year(`date`) ,sum(total_laid_off) as total
 from layoffs_staging2
 group by company,year(`date`)
 order by 3 desc;
 
 -- giving rank to the company by laid_offs
 with company_year as
 (
 select company,year(`date`) as years,sum(total_laid_off) as total
 from layoffs_staging2
 group by company,year(`date`)
 order by 3 desc
 ),company_rank as
 (
 select *,dense_rank() over (partition by years order by total desc) as ranking
 from
 company_year
 where years is not null
 )
  -- top 5 rank
 select * from 
 company_rank
 where ranking<=5
 ;
 
 
 
