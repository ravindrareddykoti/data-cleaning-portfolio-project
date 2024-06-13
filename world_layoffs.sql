SELECT DATABASE();
SHOW DATABASES ;
USE your_database_name;
USE world_layoffs;

select * from layoffs;
create table layoffs_staging
like layoffs;
select * from 
layoffs_staging;

insert layoffs_staging 
select * 
from layoffs;

-- stage 1| removing duplicates.
select *,
row_number() over( 
partition by company, industry, total_laid_off, percentage_laid_off,
`date`) as row_num
from layoffs_staging;

-- using ctes 
with duplicate_cte as 
(
select *,
row_number() over( 
partition by company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte 
where row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from 
layoffs_staging2;


insert into layoffs_staging2
select *,
row_number() over( 
partition by company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from  layoffs_staging2 
where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

delete
from  layoffs_staging2 
where row_num > 1;

select *
from  layoffs_staging2 
where row_num > 1;

SET SQL_SAFE_UPDATES = 1;

-- stage 2| standardizing the data 

select *
from  layoffs_staging2 

-- checking the column company 

select distinct(company)
from layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;

update layoffs_staging2 
set company = trim(company);

-- checking the column industry
select distinct industry 
from layoffs_staging2
order by 1;

select *
from layoffs_staging2 
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

-- checking the column location 
 select distinct location 
 from layoffs_staging2
 order by 1; -- looks good for now 
 
  -- lets check the column country 
 select distinct country 
 from layoffs_staging2
 order by 1; -- we got an issue with the country name united states which has united states. 
 
 select * 
 from layoffs_staging2
 where country like 'united states';
-- lets fix this 
select distinct country, trim(trailing '.' from country)   -- here we used trailing method to remove the exact thing from name. 
from layoffs_staging2
 order by 1;

-- lets update this 
 
 update layoffs_staging2
 set country =  trim(trailing '.' from country)
 where country like 'united states';
 
-- issue with date data type in the given data 
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2; 

update layoffs_staging2 
set `date` = str_to_date(`date`, '%m/%d/%Y');
 
select `date`
from layoffs_staging2;

-- lets change the data type 
alter table  layoffs_staging2
modify column `date` date;

-- stage 3 | removing the null values and blank spaces 

select * 
from layoffs_staging2
where total_laid_off is null;

-- we also have nullvales in percentage laidoff column 
select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- previously industry column have some blank values in it lets solve them

select * 
from layoffs_staging2
where industry = '';


-- here we trying to populate the data, ex we have aribnb , we know its a travel industry but 
-- in some rows it shows blank , so we gonna populate it 
-- using join
select * 
from layoffs_staging2
where company = 'airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company 
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;      

-- now we are changing the blank values to null values 
  
update layoffs_staging2 
set industry = null 
where industry = '';

update layoffs_staging2 t1 
join  layoffs_staging2 t2
      on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;  

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging2;

-- stage 4 removing any columns 

alter table layoffs_staging2
drop column row_num;





