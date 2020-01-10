import copy
from datetime import datetime, timedelta

# data = [
#     ["prod1", datetime(2017, 9, 8, 1, 13, 45), datetime(2018, 9, 15, 14, 14, 41), ""],
#     ["prod1", datetime(2017, 9, 8, 1, 13, 45), datetime(2999, 12, 31, 0, 0, 0), ""],
#     ]

data = [
    ["TRYAN", datetime(2018, 7, 28, 0, 33, 55), datetime(2018, 9, 26, 10, 33, 55), ""],
    ["TVEPR", datetime(2018, 8, 16, 2, 39, 9), datetime(2019, 4, 1, 1, 35, 42), ""],
    ["TVEBA", datetime(2018, 8, 16, 2, 39, 9), datetime(9999, 12, 12, 2, 39, 9), ""],
    ]

first = True
group_output = list()
account_result_list = list()
end_date_col_idx = 2
start_date_col_idx = 1
group_status_str_col_idx = 0
max_end = datetime(4000, 12, 31)
amount_of_seconds = 1
sorted_group_status_str_col_idx = 3
name_sep = ","

for current_record in data:
    if first:
        current_record[sorted_group_status_str_col_idx] = current_record[group_status_str_col_idx]
        group_output.append(current_record)
        first = False
        max_end = current_record[end_date_col_idx]
        continue

    if current_record[start_date_col_idx] > max_end:
        account_result_list += group_output
        del group_output[:]
        group_output.append(current_record)
        max_end = max(max_end, current_record[end_date_col_idx])
        continue

    need_another_loop = True
    amount_of_seconds = 1

    aux = copy.copy(current_record)
    print ('=======', aux)
    print ('=======', group_output)
    while need_another_loop:
        cnt = 0
        for old_record in group_output:
            # we overlap
            cnt +=1
            print ("count" , cnt)
            need_another_loop = False
            if old_record[start_date_col_idx] <= aux[start_date_col_idx] <= old_record[end_date_col_idx]:
                # NEW RECORD 1
                # same start not needed record 1, do not need candidate, old start < new start
                if old_record[start_date_col_idx] != aux[start_date_col_idx]:
                    new_record_1 = copy.copy(old_record)
                    new_record_1[end_date_col_idx] = aux[start_date_col_idx] + timedelta(seconds=-amount_of_seconds)
                    group_output.append(new_record_1)

                # NEW RECORD 2
                # get candidate if new end is major than first one
                candidate_r2 = old_record if old_record[end_date_col_idx] < aux[end_date_col_idx] else aux
                candidate_r3 = aux if old_record[end_date_col_idx] < aux[end_date_col_idx] else old_record
                need_another_loop = (old_record[end_date_col_idx] < aux[end_date_col_idx])
                new_record_2 = copy.copy(candidate_r2)
                new_record_2[start_date_col_idx] = aux[start_date_col_idx]

                prod_names_list = set(new_record_2[group_status_str_col_idx].split('-'))
                prod_names_list.add(candidate_r3[group_status_str_col_idx])
                sorted_prod_names_list = sorted(prod_names_list)
                new_record_2[sorted_group_status_str_col_idx] = name_sep.join(sorted_prod_names_list)
                new_record_2[group_status_str_col_idx] = name_sep.join(sorted_prod_names_list)
                group_output.append(new_record_2)
                # NEW RECORD 3
                # same end not needed record 3
                if old_record[end_date_col_idx] != aux[end_date_col_idx]:
                    new_record_3 = copy.copy(candidate_r3)
                    new_record_3[start_date_col_idx] = candidate_r2[end_date_col_idx] + \
                                                       timedelta(seconds=amount_of_seconds)
                    group_output.append(new_record_3)
                    aux = new_record_3

                max_end = max(max_end, candidate_r3[end_date_col_idx])
                group_output.remove(old_record)
                break

account_result_list += group_output

for i in account_result_list:
    print (i[1].strftime("%Y-%m-%d %H:%M:%S") , i[2].strftime("%Y-%m-%d %H:%M:%S"), i[0])

group_status = None or ""
prod_names_list = set(group_status.split(name_sep))
prod_names_list.add(None)
print (prod_names_list)
