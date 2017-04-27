h1b_app = LOAD '/project/data/*' using PigStorage('\t') AS (s_no,case_status, employer_name, soc_name, job_title, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

job_title2011 = filter h1b_app by year == '2011';
job_title2012 = filter h1b_app by year == '2012';
job_title2013 = filter h1b_app by year == '2013';
job_title2014 = filter h1b_app by year == '2014';
job_title2015 = filter h1b_app by year == '2015';
job_title2016 = filter h1b_app by year == '2016';

total_count2011 = FOREACH (GROUP job_title2011 by job_title) GENERATE group as job_title, COUNT(job_title2011);
total_count2012 = FOREACH (GROUP job_title2012 by job_title) GENERATE group as job_title, COUNT(job_title2012);
total_count2013 = FOREACH (GROUP job_title2013 by job_title) GENERATE group as job_title, COUNT(job_title2013);
total_count2014 = FOREACH (GROUP job_title2014 by job_title) GENERATE group as job_title, COUNT(job_title2014);
total_count2015 = FOREACH (GROUP job_title2015 by job_title) GENERATE group as job_title, COUNT(job_title2015);
total_count2016 = FOREACH (GROUP job_title2016 by job_title) GENERATE group as job_title, COUNT(job_title2016);

data_join1 = join total_count2011 by $0 , total_count2012 by $0, total_count2013 by $0, total_count2014 by $0, total_count2015 by $0, total_count2016 by $0;

format_data_join1 = foreach data_join1 generate $0,$1,$3,$5,$7,$9,$11;

process_data = foreach format_data_join1 generate $0,$1,$2,$3,$4,$5,$6,ROUND_TO(((float)($2-$1)/$1)*100,2),ROUND_TO(((float)($3-$2)/$2)*100,2),ROUND_TO(((float)($4-$3)/$3)*100,2),ROUND_TO(((float)($5-$4)/$4)*100,2),ROUND_TO(((float)($6-$5)/$5)*100,2);

process_data_format = foreach process_data generate $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,ROUND_TO((float)($7+$8+$9+$10+$11)/5,2);

order_format = order process_data_format by $12 DESC;

top_5 = limit order_format 5;

dump top_5;
