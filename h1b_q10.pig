h1b_app = LOAD '/project/data/*' using PigStorage('\t') AS (s_no,case_status, employer_name, soc_name, job_title, full_time_position, prevailing_wage, year, worksite, longitute, latitute);

certified_data = filter h1b_app by case_status == 'CERTIFIED';

withdrawn_data = filter h1b_app by case_status == 'CERTIFIED-WITHDRAWN';

total_count = FOREACH (GROUP h1b_app by job_title) GENERATE group as job_title, COUNT(h1b_app) as total;

formating_certified = FOREACH (GROUP certified_data by job_title) GENERATE group as job_title, COUNT(certified_data);

formating_withdrawn = FOREACH (GROUP withdrawn_data by job_title) GENERATE group as job_title, COUNT(withdrawn_data);

data_join1 = join formating_certified by $0 , formating_withdrawn by $0, total_count by $0;

format_data_join1 = foreach data_join1 generate $0,$1,$3,$5;

process_data = foreach format_data_join1 generate $0,$1,$2,$3,(float)($1+$2)/$3;

process_data_format = foreach process_data generate $0,$1,$2,$3,(float)($4)*100 as total;

filter1_1000 = filter process_data_format by $3>1000;

filter1_70 = filter filter1_1000 by $4>70.0;

final_format = order filter1_70 by $4 DESC;

store final_format into '/project/output10' using PigStorage('\t');
