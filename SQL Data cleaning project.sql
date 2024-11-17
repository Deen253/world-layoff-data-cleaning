select *
from layoffs;

-- DATA CLEANING PROJECT
create table layoff
like layoffs;

select *
from layoff;

insert into layoff
select *
from layoffs;

-- REMOVING DUPLICATE

select *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoff;

with duplicate_cte as(select *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoff)

select * 
from duplicate_cte
where row_num >1;

delete 
from duplicate_cte
where row_num >1;



CREATE TABLE `layoff2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoff2;

insert into layoff2
select *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoff;

delete
from layoff2
where row_num >1;

select *
from layoff2;

-- STANDARDIZING

select *
from layoff2;

select distinct trim(company), company
from layoff2;

update layoff2
set company = trim(company);

select distinct location
from layoff2;

select distinct location
from layoff2
where location like 'ib%';

select distinct location, 'ibadan' location2
from layoff2
where location like 'ib%';

update layoff2
set location = 'ibadan'
where location like 'ibodon';

select distinct industry
from layoff2
order by 1;

update layoff2
set industry = 'crypto'
where industry like 'crypto%';

select distinct country
from layoff2
order by 1;

update layoff2
set country = 'united states'
where country like 'united states%';

select `date`, str_to_date(`date`, '%m/%d/%Y') 
from layoff2;

update layoff2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoff2;

alter table layoff2
modify column `date` date;

-- REMOVING NULL VALUES

select *
from layoff2;

update layoff2
set industry = null
where industry = '';


select L1.company, L2.company, L1.industry, L2.industry
from layoff2 as L1
join layoff2 as L2
on L1.company = L2.company
where L1.industry is null
and L2.industry is not null;

update layoff2 L1
join layoff2 L2
on L1.company = L2.company
set L1.industry = L2.industry
where L1.industry is null
and L2.industry is not null;

select *
from layoff2
where total_laid_off is null and percentage_laid_off is null;

delete 
from layoff2
where total_laid_off is null and percentage_laid_off is null;

-- DELETING COLUMNS


alter table layoff2
drop column row_num;

select *
from layoff2;



