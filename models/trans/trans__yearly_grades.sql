with source_yearly_grades as (
    select * from {{ source('trans','yearly_grades') }}
),

final as (
    select * from source_yearly_grades
)

select * from final