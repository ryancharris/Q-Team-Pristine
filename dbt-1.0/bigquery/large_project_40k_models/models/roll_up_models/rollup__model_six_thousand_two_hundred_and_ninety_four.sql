with model_a as
  (select *
   from {{ ref('core__model_six_thousand_seven_hundred_and_thirty_two') }})
select * exclude unqiue_key,
         row_number() over (partition by 1
                            order by 1) as unqiue_key
from model_a