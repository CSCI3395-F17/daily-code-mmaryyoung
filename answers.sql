#1 CSCI1320 26
select courseid, count(quizid) as c 
from quiz_course_close_assoc 
group by courseid 
order by c DESC 
limit 1;

select * from courses where courseid = 12;

#2 CSCI1321 65
select qc.courseid, count(*) as c
from (
    select quizid, func_question_id from function_assoc
    union all
    select quizid, lambda_question_id from lambda_assoc
    union all 
    select quizid, expr_question_id from expression_assoc
    union all
    select quizid, mc_question_id from multiple_choice_assoc
) x
join quiz_course_close_assoc as qc
on qc.quizid = x.quizid
group by qc.courseid 
order by c DESC limit 1;
select code from courses where courseid = 11;

#3 0.04
select count(*) from code_answers where question_type = 3 and correct = 1;
select count(*) from code_answers where question_type = 3;
select 618.0/3746;

#4 9
select count(distinct courseid) 
from quiz_course_close_assoc 
join lambda_assoc 
on lambda_assoc.quizid = quiz_course_close_assoc.quizid;

#5 abcde correct 126
select userid, count(correct) 
from (
    select userid, correct 
    from mc_answers where correct = 1 
    union all 
    select userid, correct 
    from code_answers where correct = 1) x 
group by userid 
order by count(correct) DESC 
limit 1;

select * from users where userid = 156;


#6 type 0 occuren 13
select spec_type, count(*) as c 
from variable_specifications 
group by spec_type 
order by c DESC 
limit 1;



#7 0.4
select count(*)
from(
    select quizid from function_assoc
    union
    select quizid from lambda_assoc
    union
    select quizid from expression_assoc
    ) x;
select count(distinct quizid) from quizzes;
select 22/55;

#8 1.96
select count(*) from multiple_choice_questions;
select count(distinct quizid) from quizzes;
select 108/55;

#9 0.8
select count(*)
from(
    select func_question_id from function_assoc
    union all
    select lambda_question_id from lambda_assoc
    union all
    select expr_question_id from expression_assoc
) x;
select count(distinct quizid) from quizzes;
select 44/55;

#10 courseid 1 quizid 21 avg 349.5
select y.courseid, y.quizid, y.c/ count(*) as avg
from(
    select courseid, qc.quizid, count(*) as c
    from(
        select quizid from mc_answers
        union all
        select quizid from code_answers
    ) x
    join quiz_course_close_assoc as qc
    on qc.quizid = x.quizid
    group by courseid, qc.quizid) y
join user_course_assoc as uc
on uc.courseid = y.courseid
group by courseid, quizid
order by avg DESC
limit 1
;
