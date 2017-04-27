h1b_app = LOAD '/project/data/*' using PigStorage('\t') AS (s_no,case_status, employer_name, soc_name, job_title, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

job_title2011 = filter h1b_app by year == '2011';
job_title2012 = filter h1b_app by year == '2012';
job_title2013 = filter h1b_app by year == '2013';
job_title2014 = filter h1b_app by year == '2014';
job_title2015 = filter h1b_app by year == '2015';
job_title2016 = filter h1b_app by year == '2016';
job_titlenull = filter h1b_app by year == '\\N';

total_count = FOREACH (GROUP h1b_app ALL) GENERATE COUNT(h1b_app);

group2011 = GROUP job_title2011 by (case_status, year);
group2012 = GROUP job_title2012 by (case_status, year);
group2013 = GROUP job_title2013 by (case_status, year);
group2014 = GROUP job_title2014 by (case_status, year);
group2015 = GROUP job_title2015 by (case_status, year);
group2016 = GROUP job_title2016 by (case_status, year);
groupnull = GROUP job_titlenull by (case_status, year);

total_count2011 = FOREACH group2011 GENERATE flatten(group) as (case_status, year), COUNT(job_title2011);
total_count2012 = FOREACH group2012 GENERATE flatten(group) as (case_status, year), COUNT(job_title2012);
total_count2013 = FOREACH group2013 GENERATE flatten(group) as (case_status, year), COUNT(job_title2013);
total_count2014 = FOREACH group2014 GENERATE flatten(group) as (case_status, year), COUNT(job_title2014);
total_count2015 = FOREACH group2015 GENERATE flatten(group) as (case_status, year), COUNT(job_title2015);
total_count2016 = FOREACH group2016 GENERATE flatten(group) as (case_status, year), COUNT(job_title2016);
total_countnull = FOREACH groupnull GENERATE flatten(group) as (case_status, year), COUNT(job_titlenull);

data_union1 = union total_count2011, total_count2012, total_count2013, total_count2014, total_count2015, total_count2016, total_countnull;

format_data_union1 = foreach data_union1 generate $0,$1,$2;

calc_union1 = foreach data_union1 generate $0,$1,$2,total_count.$0,(double)((float)$2/(float)total_count.$0) as value;

final_calc = foreach calc_union1 generate $0,$1,$2,$3,(double)ROUND_TO($4*100,8) as percentage;

order_final_calc = order final_calc by $0 ASC, $1 ASC;

