with model_a as
  (select *
   from {{ ref('core__model_three_thousand_five_hundred_and_fifty') }})
select * exclude unqiue_key,
         row_number() over (partition by 1
                            order by 1) as unqiue_key
from model_a