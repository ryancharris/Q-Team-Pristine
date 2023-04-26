with model_a as
  (select *
   from {{ ref('rollup__model_one_thousand,_one_hundred_and_fifty_five') }})
select * exclude unqiue_key,
         row_number() over (partition by 1
                            order by 1) as unqiue_key
from model_a