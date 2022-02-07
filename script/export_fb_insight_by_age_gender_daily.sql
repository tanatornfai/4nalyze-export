select 
datetime,
to_char(datetime, 'YYYYMMDD') as date_code,
to_char(datetime, 'YYYYMM')as month_year_code,
iag.source_id,
sou.source_id as source_name,
age,
gender,
case 
when gender = 'Male' then '01' 
when gender = 'Female' then '02' 
when gender = 'Unknown' then '99' 
end
as gender_code,
page_fans_gender_age,
page_content_activity_unique,
page_places_checkins,
page_impressions_unique
from "social_4nalyzePOCSPWG_3".fb_insight_by_age_gender_daily iag
left join "social_4nalyzePOCSPWG_3".source sou on iag.source_id = sou.id  
where datetime >= '${prev_ds}'::date and datetime < '${ds}'::date