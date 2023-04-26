with model_a as
  (select * exclude unqiue_key,
            unqiue_key as model_a_unqiue_key
   from {{ ref('int__marketing_model_four_thousand_one_hundred_and_nine') }}),
     model_b as
  (select * exclude unqiue_key,
            unqiue_key as model_b_unqiue_key
   from {{ ref('stg__medicare_sample_data_medicare_samples__2009_beneficiary_summary_file_sample') }}),
     model_c as
  (select * exclude unqiue_key,
            unqiue_key as model_c_unqiue_key
   from {{ ref('int__marketing_model_three_thousand_two_hundred_and_ninety_two') }}),
     joined_models as
  (select a.*,
          b.*,
          c.*
   from model_a a
   inner join model_b b on a.model_a_unqiue_key = b.model_b_unqiue_key
   inner join model_c c on a.model_a_unqiue_key = b.model_c_unqiue_key)
select * exclude (model_a_unqiue_key, model_b_unqiue_key, model_c_unqiue_key),
         row_number() over (partition by 1
                            order by 1) as unqiue_key
from joined_models