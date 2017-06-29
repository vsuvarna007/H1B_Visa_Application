#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${NUMBER} The H1B is an employment-based, non-immigrant visa category for temporary foreign workers in the United States. For a foreign national to apply for H1B visa, an US employer must offer a job and petition for H1B visa with the US immigration department. This is the most common visa status applied for and held by international students once they complete college/ higher education (Masters, Ph.D.) and work in a full-time position."
    echo -e ""
    echo -e "${MENU}**********************Analysis MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1a)${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1b)${MENU} Find top 5 job titles who are having highest growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2a)${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2b)${MENU} Find top 5 locations in the US who have got certified visa for each year. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} Which industry has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} Which top 5 employers file the most petitions each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} Find the percentage and the count of each case status on total applications for each year. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} The number of applications for each year ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} ind the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order.  ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} Which are  employers along with the number of petitions who have the success rate more than 70%  in petitions and total petitions filed more than 1000? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions and total petitions filed more than 1000? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11)${MENU} Export result for question no 10 to MySql database. ${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1a) clear;
        option_picked "Is the number of petitions with Data Engineer job title increasing over time? (MR)";
        hadoop jar /home/hduser/h1b.jar h1b.app.DE /user/hive/warehouse/project.db/h1b_final/* /project/question1a
	hadoop fs -cat /project/question1a/p*
        show_menu;
        ;;

        1b) clear;
        option_picked "Find top 5 job titles who are having highest growth in applications. (Pig)";
        pig -f /home/hduser/Downloads/question1b.pig
        show_menu;
        ;;
            
        2a) clear;
        option_picked "Which part of the US has the most Data Engineer jobs for each year? ";
        hadoop jar /home/hduser/h1b.jar h1b.app.DE2 /user/hive/warehouse/project.db/h1b_final/* /project/question2a
	hadoop fs -cat /project/question2a/p*
        show_menu;
        ;;
	
        2b) clear;
        option_picked "Find top 5 locations in the US who have got certified visa for each year.";
	hive -f /home/hduser/Desktop/project/question2a.sql
        show_menu;
        ;;
            
	3) clear;
        option_picked "Which industry has the most number of Data Scientist positions? (MR)";
       	hadoop jar /home/hduser/h1b.jar h1b.app.DataScientist /user/hive/warehouse/project.db/h1b_final/* /project/question3
	hadoop fs -cat /project/question3/p*
        show_menu;
        ;;
	
	4) clear;
        option_picked "Which top 5 employers file the most petitions each year?";
	hive -f /home/hduser/Desktop/project/question4.sql
        show_menu;
        ;;

5) clear;
        option_picked "Find the most popular top 10 job positions for H1B visa applications for each year?";
	hive -f /home/hduser/Desktop/project/question5.sql
        show_menu;
        ;;

6) clear;
        option_picked "Find the percentage and the count of each case status on total applications for each year.";
	pig -f /home/hduser/Desktop/question6.pig
        show_menu;
        ;;

7) clear;
        option_picked "Create a bar graph to depict the number of applications for each year";
	hadoop jar /home/hduser/h1b.jar h1b.app.ApplicationNumbers /user/hive/warehouse/project.db/h1b_final/* /project/question7
	hadoop fs -cat /project/question7/p* 
        show_menu;
        ;;

8) clear;
        option_picked "Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order.";
        hive -f /home/hduser/Desktop/project/question8.sql
        show_menu;
        ;;

9) clear;
        option_picked "Which are  employers along with the number of petitions who have the success rate more than 70%  in petitions and total petitions filed more than 1000? (Pig)";
	pig -f /home/hduser/Desktop/question9.pig
        show_menu;
        ;;

10) clear;
        option_picked "Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions and total petitions filed more than 1000?";
	pig -f /home/hduser/Desktop/question10.pig
        show_menu;
        ;;

11) clear;
        option_picked "Export result for question no 10 to MySql database. ";
	mysql -u root -p project < /home/hduser/Desktop/create.sql
        sqoop export --connect jdbc:mysql://localhost/project --username  root --password 'hadoop' --table job_sucess --update-mode allowinsert --update-key employer_name --export-dir /project/output10/part-r-00000 --input-fields-terminated-by '\t';
	mysql -u root -p project < /home/hduser/Desktop/show.sql
        show_menu;
        ;;	

        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done


