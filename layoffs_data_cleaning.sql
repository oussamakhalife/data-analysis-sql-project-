SELECT * 
FROM layoffs;

create table layoffs_staging 
Like layoffs ; 

select *
from layoffs_staging ; 

Insert layoffs_staging
select *
from layoffs ; 



SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
       ) AS row_numb
FROM layoffs_staging;


with duplicate_cte as 
(
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location , industry, total_laid_off, percentage_laid_off, `date` , stage , country , funds_raised_millions
       ) AS row_numb
FROM layoffs_staging
)
select *
from duplicate_cte 
where row_numb > 1 ; 

select *
from layoffs_staging  
where company = 'Casper' ; 




CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  `row_numb` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging3 ;

insert into layoffs_staging3
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location , industry, total_laid_off, percentage_laid_off, `date` , stage , country , funds_raised_millions
       ) AS row_numb
FROM layoffs_staging;

 select *
 from layoffs_staging3
 where row_numb > 1;


 delete 
 from layoffs_staging3
 where row_numb > 1;

 select *
 from layoffs_staging3
 where row_numb > 1;


 Delete 
 from layoffs_staging3
 where row_numb > 1;



 select *
 from layoffs_staging3;


select  company , TRIM(company)
 from layoffs_staging3;


update layoffs_staging3
set company = trim(company);



 select distinct industry
 from layoffs_staging3;
 
 
select *
from layoffs_staging3
 where industry like 'crypto%' ;
 
 update layoffs_staging3
 set industry = 'crypto'
 where industry like 'crypto%' ;
 
 
 
 
 select distinct country , trim(trailing '.' from country)
 from layoffs_staging3
 order by 1 ; 
 
  update layoffs_staging3
 set country =  trim(trailing '.' from country)
 where country like 'United States%' ;
 
 
 select `date` 
 From layoffs_staging3;
 
 
 update layoffs_staging3
 set `date` = str_to_date(`date`,'%m/%d/%Y');
 
 
 alter table layoffs_staging3
 Modify Column `date` DATE ; 
 
 
 select *
  From layoffs_staging3
  where total_laid_off is null 
  and percentage_laid_off is null ;
  
  update  layoffs_staging3
  set industry = null 
  where industry = '';
  
  
  select *
  from layoffs_staging3
  where industry is null 
  or industry = '' ; 
  
  select * 
  from layoffs_staging3
  where company = 'Airbnb';
  
select t1.industry , t2.industry
from layoffs_staging3 t1
join layoffs_staging3 t2 
	on t1.company = t2.company
where t1.industry is null 
and t2.industry is not null ;



UPDATE layoffs_staging3 t1 
join layoffs_staging3 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null ;


SELECT * 
from layoffs_staging3 
where total_laid_off is null 
and percentage_laid_off is null ;



Delete  
from layoffs_staging3 
where total_laid_off is null 
and percentage_laid_off is null ;


alter table layoffs_staging3
Drop column row_numb;


select *
from layoffs_staging3
