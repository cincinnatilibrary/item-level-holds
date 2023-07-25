import json
import csv
from datetime import datetime, timedelta
import os

from sierra_db import execute_query_yield_rows, get_cursor
from chpl_email import send_email


try:
    with open('config.json') as f:
        config = json.load(f)
        dsn = config['dsn']
except Exception as e:
    print(e)
    
directory = "./output"

# Check if the directory exists
if not os.path.exists(directory):
    # If the directory doesn't exist, create it
    os.makedirs(directory)
    
# Get the current date and format it as a string
date_str = datetime.now().strftime("%Y-%m-%d")


# Now you can use this directory to save your file
filename = f"item_level_holds_{date_str}.csv"
filepath = os.path.join(directory, filename)


sql = """\
with data as (
    select
        h.id,
        -- h.record_id as item_record_id,
        (
            select 
                v.field_content 
            from
                sierra_view.varfield as v
            where 
                v.record_id = h.record_id
                and v.varfield_type_code = 'b'
            order by 
                v.occ_num 
            limit 1
        ) as item_barcode,
        --h.patron_record_id ,
        (
            select
                json_agg(v.field_content order by v.occ_num)
            from 
                sierra_view.varfield as v
            where 
                v.record_id = h.patron_record_id 
                and v.varfield_type_code = 'b'
            -- order by 
            --     v.occ_num 
            -- limit 1
        ) as patron_barcode,
        pr.ptype_code as ptype,
        (
            select 
                rm2.record_type_code || rm2.record_num || 'a'
            from 
                sierra_view.record_metadata as rm2 
            where 
                rm2.id = h.record_id
        ) as item_record_num,
        i.item_status_code ,
        --h.status ,
        h.on_holdshelf_gmt ,
        c.checkout_gmt ,
        c.loanrule_code_num ,
        (
            select 
                rm2.record_type_code || rm2.record_num || 'a'
            from 
            sierra_view.record_metadata as rm2 
            where 
                rm2.id = vrirl.volume_record_id 
            limit 1
        ) as vol_record,
        --brp.bib_record_id ,
        (
            select 
                rm2.record_type_code || rm2.record_num || 'a'
            from 
                sierra_view.record_metadata as rm2
            where 
                rm2.id = brirl.bib_record_id 
            limit 1
        ) as bib_record_num,
        (
            select 
                v.field_content 
            from 
                sierra_view.varfield as v
            where 
                v.record_id = vrirl.volume_record_id
                and v.varfield_type_code = 'v'
        ) as volume_statement,
        --c.due_gmt ,
        --c.checkout_gmt ,
        --vrirl.volume_record_id ,
        h.placed_gmt,
        (
            select 
                coalesce(prf.last_name || ', ', '')
                    || coalesce(prf.first_name  || ' ', '')
                    || coalesce(prf.middle_name, '')
            from 
                sierra_view.patron_record_fullname as prf
            where 
                prf.patron_record_id = h.patron_record_id
            order by 
                prf.id
            limit 1
        ) as full_name,
        h.pickup_location_code ,
        brp.best_title ,
        brp.best_author,
        h.note
    from 
        sierra_view.hold as h
        join sierra_view.record_metadata as rm on (
            rm.id = h.record_id 
            and rm.record_type_code = 'i'   -- limit to item-level
            and rm.campus_code = ''         -- item is not a "virtual" item
        )
        join sierra_view.item_record as i on
            i.record_id = h.record_id
        join sierra_view.bib_record_item_record_link as brirl on brirl.item_record_id = i.record_id 
        join sierra_view.bib_record_property as brp on brp.bib_record_id = brirl.bib_record_id 
        join sierra_view.patron_record as pr on
            pr.record_id = h.patron_record_id 
        left outer join sierra_view.checkout as c on
            c.item_record_id = h.record_id  -- item is checked out or not
        left outer join sierra_view.volume_record_item_record_link as vrirl on 
            vrirl.item_record_id = h.record_id
    where
        (
            item_status_code NOT IN ('t', '!', '(')
            or (
                item_status_code = '-'
                and checkout_gmt IS NOT NULL
            )
        )
        or (
            i.item_status_code = '-'
            and c.checkout_gmt IS null
        )
)
select
    'item_on_shelf' as type,
    *
from 
    data
where
    (
        data.item_status_code = '-'
        and data.checkout_gmt IS NULL
    )


union all


select
    'item_not_circ_or_checked_out' as type,
    *
from
    data
where
    data.id not in (
        -- the query before the union
        select
            id
        from
            data
        where
            (
                item_status_code = '-'
                and checkout_gmt IS NULL
            )
    )
order by
    type,
    ptype,
    placed_gmt
"""

with get_cursor(dsn=dsn) as cursor:
    rows = execute_query_yield_rows(cursor, sql, None)
    
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        columns = next(rows)
        writer.writerow(columns)
        for i, row in enumerate(rows):
            writer.writerow(row)
            if i % 1000 == 0:
                print('.', end='')
        print(f'.done ({i+1})')


send_email(
    smtp_username=config['smtp_username'], 
    smtp_password=config['smtp_password'], 
    subject="Item-level Holds", 
    message="See attached.", 
    from_addr="ray.voelker@chpl.org", 
    to_addr=config['send_list'], 
    files=[filepath]
)

